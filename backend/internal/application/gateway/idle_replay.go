package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/chenyme/grok2api/backend/internal/domain/audit"
)

var errIdleReplayUnavailable = errors.New("cached upstream response is unavailable")

const (
	idleReplayTTL     = 2 * time.Minute
	idleReplayMaxSize = 8 << 20
)

// idleReplayStore keeps one upstream generation for a streaming request so a
// client that times out on upstream_stream_idle_timeout can download the bytes
// already produced instead of starting another model call.
type idleReplayStore struct {
	mu      sync.Mutex
	entries map[string]*idleReplayEntry
}

type idleReplayEntry struct {
	mu        sync.Mutex
	cond      *sync.Cond
	store     *idleReplayStore
	key       string
	status    int
	header    http.Header
	buf       bytes.Buffer
	aliases   []string
	published bool
	done      bool
	dropped   bool
	expires   time.Time
}

func idleReplayKey(input Input) string {
	return idleReplayDigest("body", input.ClientKey.ID, input.PublicModel, string(input.Body))
}

func idleReplaySessionKey(input Input) string {
	session := strings.TrimSpace(input.PromptCacheKey)
	if session == "" {
		session = strings.TrimSpace(input.PromptCacheSeed)
	}
	if session == "" {
		return ""
	}
	return idleReplayDigest("session", input.ClientKey.ID, input.PublicModel, session)
}

func idleReplayDigest(kind string, clientKeyID uint64, model, material string) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte(kind))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(strconv.FormatUint(clientKeyID, 10)))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(model))
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write([]byte(material))
	return hex.EncodeToString(digest.Sum(nil))
}

func shouldShareIdleReplay(input Input) bool {
	if !input.Streaming || input.ForcedAccountID != 0 || input.ForcedEgressNodeID != 0 || input.PreviousResponseID != "" {
		return false
	}
	switch input.Operation {
	case "", audit.OperationResponses, audit.OperationChat, audit.OperationMessages:
		return true
	default:
		return false
	}
}

func (s *idleReplayStore) begin(key string, now time.Time) (*idleReplayEntry, bool) {
	s.mu.Lock()
	if s.entries == nil {
		s.entries = make(map[string]*idleReplayEntry)
	}
	existing := s.entries[key]
	s.mu.Unlock()
	if existing != nil {
		existing.mu.Lock()
		expired := existing.done && (existing.dropped || !existing.expires.After(now) || existing.buf.Len() == 0)
		existing.mu.Unlock()
		if !expired {
			return existing, false
		}
		s.mu.Lock()
		if current := s.entries[key]; current == existing {
			delete(s.entries, key)
		}
		s.mu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.entries[key]; current != nil {
		return current, false
	}
	entry := &idleReplayEntry{store: s, key: key, status: http.StatusOK}
	entry.cond = sync.NewCond(&entry.mu)
	s.entries[key] = entry
	return entry, true
}

func (e *idleReplayEntry) tee(status int, header http.Header, body io.ReadCloser) io.ReadCloser {
	if e == nil || body == nil {
		return body
	}
	e.mu.Lock()
	e.status = status
	if header != nil {
		e.header = header.Clone()
	}
	e.published = true
	e.cond.Broadcast()
	e.mu.Unlock()
	return &idleReplayTee{entry: e, inner: body}
}

func (e *idleReplayEntry) append(data []byte) {
	if len(data) == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.done {
		return
	}
	room := idleReplayMaxSize - e.buf.Len()
	if room <= 0 {
		return
	}
	if len(data) > room {
		data = data[:room]
	}
	_, _ = e.buf.Write(data)
	e.cond.Broadcast()
}

func (e *idleReplayEntry) finish() {
	e.mu.Lock()
	if e.done {
		e.mu.Unlock()
		return
	}
	e.done = true
	drop := e.buf.Len() == 0
	if drop {
		e.dropped = true
	} else {
		e.expires = time.Now().Add(idleReplayTTL)
	}
	e.cond.Broadcast()
	e.mu.Unlock()
	if drop {
		e.store.deleteEntry(e)
	}
}

func (e *idleReplayEntry) abandonIfUnpublished() {
	e.mu.Lock()
	published := e.published
	e.mu.Unlock()
	if published {
		return
	}
	e.finish()
}

func (s *idleReplayStore) delete(key string, entry *idleReplayEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if current := s.entries[key]; current == entry {
		delete(s.entries, key)
	}
}

func (s *idleReplayStore) deleteEntry(entry *idleReplayEntry) {
	if entry == nil {
		return
	}
	entry.mu.Lock()
	keys := append([]string{entry.key}, entry.aliases...)
	entry.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, key := range keys {
		if current := s.entries[key]; current == entry {
			delete(s.entries, key)
		}
	}
}

func (s *idleReplayStore) detach(key string, entry *idleReplayEntry) {
	if key == "" || entry == nil {
		return
	}
	s.mu.Lock()
	if current := s.entries[key]; current == entry {
		delete(s.entries, key)
	}
	s.mu.Unlock()
}

func (s *idleReplayStore) alias(key string, entry *idleReplayEntry) {
	if key == "" || entry == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		s.entries = make(map[string]*idleReplayEntry)
	}
	s.entries[key] = entry
	entry.mu.Lock()
	entry.aliases = append(entry.aliases, key)
	entry.mu.Unlock()
}

func (s *idleReplayStore) get(key string, now time.Time) *idleReplayEntry {
	if key == "" {
		return nil
	}
	s.mu.Lock()
	existing := s.entries[key]
	s.mu.Unlock()
	if existing == nil {
		return nil
	}
	existing.mu.Lock()
	expired := existing.done && (existing.dropped || !existing.expires.After(now) || existing.buf.Len() == 0)
	existing.mu.Unlock()
	if !expired {
		return existing
	}
	s.delete(key, existing)
	return nil
}

func (e *idleReplayEntry) follow(ctx context.Context) (*Result, error) {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			e.mu.Lock()
			e.cond.Broadcast()
			e.mu.Unlock()
		case <-stop:
		}
	}()
	e.mu.Lock()
	for !e.published && !e.done {
		if ctx.Err() != nil {
			e.mu.Unlock()
			return nil, ctx.Err()
		}
		e.cond.Wait()
	}
	if !e.published && e.buf.Len() == 0 {
		e.mu.Unlock()
		return nil, errIdleReplayUnavailable
	}
	status := e.status
	var header http.Header
	if e.header != nil {
		header = e.header.Clone()
	} else {
		header = make(http.Header)
	}
	e.mu.Unlock()
	if status == 0 {
		status = http.StatusOK
	}
	return &Result{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     header,
		Body:       &idleReplayReader{entry: e},
		Finalize:   func(Usage, string, string) {},
	}, nil
}

type idleReplayTee struct {
	entry *idleReplayEntry
	inner io.ReadCloser
	once  sync.Once
}

func (t *idleReplayTee) Read(buffer []byte) (int, error) {
	n, err := t.inner.Read(buffer)
	if n > 0 {
		t.entry.append(buffer[:n])
	}
	if err != nil {
		t.entry.finish()
	}
	return n, err
}

func (t *idleReplayTee) Close() error {
	t.entry.finish()
	var err error
	t.once.Do(func() { err = t.inner.Close() })
	return err
}

type idleReplayReader struct {
	entry  *idleReplayEntry
	offset int
}

func (r *idleReplayReader) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	r.entry.mu.Lock()
	defer r.entry.mu.Unlock()
	for r.offset >= r.entry.buf.Len() && !r.entry.done {
		r.entry.cond.Wait()
	}
	if r.offset >= r.entry.buf.Len() {
		return 0, io.EOF
	}
	n := copy(buffer, r.entry.buf.Bytes()[r.offset:])
	r.offset += n
	return n, nil
}

func (r *idleReplayReader) Close() error { return nil }

func (s *Service) beginIdleReplay(input *Input) (*idleReplayEntry, bool) {
	if s == nil || input == nil || !shouldShareIdleReplay(*input) {
		return nil, false
	}
	now := time.Now()
	exact := idleReplayKey(*input)
	if existing := s.idleReplay.get(exact, now); existing != nil {
		return existing, false
	}
	session := idleReplaySessionKey(*input)
	if session != "" && isContinuationRequest(input.Body) {
		if pending := s.idleReplay.get(session, now); pending != nil {
			pending.mu.Lock()
			done := pending.done
			snapshot := append([]byte(nil), pending.buf.Bytes()...)
			pending.mu.Unlock()
			if !done {
				// The stalled turn is still streaming. "continue" wants that
				// generation, not a second model call.
				return pending, false
			}
			text, complete := idleReplayTranscript(snapshot)
			if text != "" && complete {
				s.idleReplay.detach(session, pending)
				return pending, false
			}
			if text != "" {
				if patched, ok := spliceIdleContinuation(input.Body, text); ok {
					s.idleReplay.detach(session, pending)
					input.Body = patched
					entry, leader := s.idleReplay.begin(idleReplayKey(*input), now)
					if leader {
						s.idleReplay.alias(session, entry)
					}
					return entry, leader
				}
			}
		}
	}
	entry, leader := s.idleReplay.begin(exact, now)
	if leader {
		s.idleReplay.alias(session, entry)
	}
	return entry, leader
}

func isContinuationRequest(body []byte) bool {
	return isContinuationCue(lastUserText(body))
}

func isContinuationCue(text string) bool {
	text = strings.TrimSpace(text)
	text = strings.Trim(text, " \t\r\n.。!！?？~～,，、")
	if text == "" || utf8.RuneCountInString(text) > 24 {
		return false
	}
	switch strings.ToLower(text) {
	case "continue", "continued", "go on", "keep going", "carry on", "resume", "please continue",
		"继续", "请继续", "继续说", "继续吧", "接着", "接着说", "往下", "往下说", "然后呢":
		return true
	default:
		return false
	}
}

func lastUserText(body []byte) string {
	var root map[string]json.RawMessage
	if json.Unmarshal(body, &root) != nil {
		return ""
	}
	if raw, ok := root["messages"]; ok {
		if text := lastRoleText(raw); text != "" {
			return text
		}
	}
	raw, ok := root["input"]
	if !ok {
		return ""
	}
	var asString string
	if json.Unmarshal(raw, &asString) == nil {
		return strings.TrimSpace(asString)
	}
	return lastResponsesUserText(raw)
}

func lastRoleText(raw json.RawMessage) string {
	var messages []map[string]json.RawMessage
	if json.Unmarshal(raw, &messages) != nil {
		return ""
	}
	for index := len(messages) - 1; index >= 0; index-- {
		var role string
		_ = json.Unmarshal(messages[index]["role"], &role)
		if strings.EqualFold(strings.TrimSpace(role), "user") {
			return strings.TrimSpace(flattenMessageContent(messages[index]["content"]))
		}
	}
	return ""
}

func lastResponsesUserText(raw json.RawMessage) string {
	var items []map[string]json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		return ""
	}
	for index := len(items) - 1; index >= 0; index-- {
		var role, typeName string
		_ = json.Unmarshal(items[index]["role"], &role)
		_ = json.Unmarshal(items[index]["type"], &typeName)
		role = strings.ToLower(strings.TrimSpace(role))
		typeName = strings.TrimSpace(typeName)
		if role == "user" && (typeName == "" || typeName == "message") {
			return strings.TrimSpace(flattenMessageContent(items[index]["content"]))
		}
	}
	return ""
}

func idleReplayTranscript(payload []byte) (text string, complete bool) {
	var deltas, finals strings.Builder
	for _, line := range bytes.Split(payload, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		raw := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if bytes.Equal(raw, []byte("[DONE]")) {
			complete = true
			continue
		}
		var event map[string]json.RawMessage
		if json.Unmarshal(raw, &event) != nil {
			continue
		}
		var eventType string
		_ = json.Unmarshal(event["type"], &eventType)
		switch strings.TrimSpace(eventType) {
		case "response.completed", "response.done", "message_stop":
			complete = true
		case "response.output_text.delta":
			var delta string
			if json.Unmarshal(event["delta"], &delta) == nil {
				deltas.WriteString(delta)
			}
		case "content_block_delta":
			var delta struct {
				Type string `json:"type"`
				Text string `json:"text"`
			}
			if json.Unmarshal(event["delta"], &delta) == nil && delta.Type == "text_delta" {
				deltas.WriteString(delta.Text)
			}
		case "response.output_item.done":
			finals.WriteString(responsesMessageText(event["item"]))
		}
		var chat struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
		}
		if json.Unmarshal(raw, &chat) == nil {
			for _, choice := range chat.Choices {
				deltas.WriteString(choice.Delta.Content)
				if strings.TrimSpace(choice.FinishReason) != "" {
					complete = true
				}
			}
		}
	}
	if text = strings.TrimSpace(deltas.String()); text != "" {
		return text, complete
	}
	return strings.TrimSpace(finals.String()), complete
}

func responsesMessageText(raw json.RawMessage) string {
	var item struct {
		Type    string `json:"type"`
		Role    string `json:"role"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if json.Unmarshal(raw, &item) != nil || item.Type != "message" {
		return ""
	}
	var text strings.Builder
	for _, part := range item.Content {
		if part.Type == "" || part.Type == "output_text" || part.Type == "text" {
			text.WriteString(part.Text)
		}
	}
	return text.String()
}

func spliceIdleContinuation(body []byte, partial string) ([]byte, bool) {
	partial = strings.TrimSpace(partial)
	if partial == "" {
		return nil, false
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		return nil, false
	}
	if messages, ok := payload["messages"].([]any); ok {
		index := lastUserAnyIndex(messages)
		if index < 0 || assistantAlreadyHas(messages, index, partial) {
			return nil, false
		}
		payload["messages"] = insertAny(messages, index, map[string]any{"role": "assistant", "content": partial})
		encoded, err := json.Marshal(payload)
		return encoded, err == nil
	}
	switch input := payload["input"].(type) {
	case []any:
		index := lastResponsesUserAnyIndex(input)
		if index < 0 || assistantAlreadyHas(input, index, partial) {
			return nil, false
		}
		payload["input"] = insertAny(input, index, map[string]any{"type": "message", "role": "assistant", "content": partial})
	case string:
		if !isContinuationCue(input) {
			return nil, false
		}
		payload["input"] = []any{
			map[string]any{"type": "message", "role": "assistant", "content": partial},
			map[string]any{"type": "message", "role": "user", "content": strings.TrimSpace(input)},
		}
	default:
		return nil, false
	}
	encoded, err := json.Marshal(payload)
	return encoded, err == nil
}

func lastUserAnyIndex(messages []any) int {
	for index := len(messages) - 1; index >= 0; index-- {
		message, ok := messages[index].(map[string]any)
		if !ok {
			continue
		}
		role, _ := message["role"].(string)
		if strings.EqualFold(strings.TrimSpace(role), "user") {
			return index
		}
	}
	return -1
}

func lastResponsesUserAnyIndex(items []any) int {
	for index := len(items) - 1; index >= 0; index-- {
		item, ok := items[index].(map[string]any)
		if !ok {
			continue
		}
		role, _ := item["role"].(string)
		typeName, _ := item["type"].(string)
		role = strings.ToLower(strings.TrimSpace(role))
		typeName = strings.TrimSpace(typeName)
		if role == "user" && (typeName == "" || typeName == "message") {
			return index
		}
	}
	return -1
}

func assistantAlreadyHas(items []any, userIndex int, partial string) bool {
	if userIndex <= 0 {
		return false
	}
	return visibleItemText(items[userIndex-1]) == partial
}

func visibleItemText(value any) string {
	item, ok := value.(map[string]any)
	if !ok {
		return ""
	}
	role, _ := item["role"].(string)
	if !strings.EqualFold(strings.TrimSpace(role), "assistant") {
		return ""
	}
	switch content := item["content"].(type) {
	case string:
		return strings.TrimSpace(content)
	default:
		encoded, err := json.Marshal(content)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(flattenMessageContent(encoded))
	}
}

func insertAny(items []any, index int, value any) []any {
	next := make([]any, 0, len(items)+1)
	next = append(next, items[:index]...)
	next = append(next, value)
	next = append(next, items[index:]...)
	return next
}

// replayPrefix reports whether an idle-timeout buffer already holds response
// bytes. Empty buffers are closed. A non-empty buffer is returned intact so
// the caller can hand it to the client without another model call.
func replayPrefix(body io.ReadCloser) (io.ReadCloser, bool) {
	if body == nil {
		return nil, false
	}
	probe := make([]byte, 1)
	n, err := body.Read(probe)
	if n == 0 {
		_ = body.Close()
		return nil, false
	}
	return &prefixReadCloser{prefix: append([]byte(nil), probe[:n]...), rest: body, pending: err}, true
}

type prefixReadCloser struct {
	prefix  []byte
	rest    io.ReadCloser
	pending error
}

func (r *prefixReadCloser) Read(buffer []byte) (int, error) {
	if len(r.prefix) > 0 {
		n := copy(buffer, r.prefix)
		r.prefix = r.prefix[n:]
		if len(r.prefix) == 0 && r.pending != nil && n > 0 {
			err := r.pending
			r.pending = nil
			if err == io.EOF {
				return n, nil
			}
		}
		return n, nil
	}
	if r.pending != nil {
		err := r.pending
		r.pending = nil
		return 0, err
	}
	return r.rest.Read(buffer)
}

func (r *prefixReadCloser) Close() error { return r.rest.Close() }
