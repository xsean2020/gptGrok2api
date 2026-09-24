package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/audit"
	clientkeydomain "github.com/chenyme/grok2api/backend/internal/domain/clientkey"
)

func TestIdleReplayServesRetryWithoutAnotherLeader(t *testing.T) {
	var store idleReplayStore
	input := Input{
		ClientKey:   clientkeydomain.Key{ID: 7},
		PublicModel: "grok-4.5",
		Body:        []byte(`{"input":"hello"}`),
		Streaming:   true,
		Operation:   audit.OperationResponses,
	}
	if !shouldShareIdleReplay(input) {
		t.Fatal("streaming responses should be replayed after an idle timeout")
	}
	key := idleReplayKey(input)
	leader, isLeader := store.begin(key, time.Now())
	if !isLeader {
		t.Fatal("first request should own the generation")
	}
	body := leader.tee(http.StatusOK, http.Header{"Content-Type": []string{"text/event-stream"}}, io.NopCloser(strings.NewReader("data: cached\n\n")))
	follower, isFollower := store.begin(key, time.Now())
	if isFollower || follower != leader {
		t.Fatal("retry should join the cached generation")
	}
	result, err := follower.follow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cachedCh := make(chan string, 1)
	errCh := make(chan error, 1)
	go func() {
		cached, readErr := io.ReadAll(result.Body)
		if readErr != nil {
			errCh <- readErr
			return
		}
		cachedCh <- string(cached)
	}()
	original, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	_ = body.Close()
	var cached string
	select {
	case cached = <-cachedCh:
	case readErr := <-errCh:
		t.Fatal(readErr)
	case <-time.After(2 * time.Second):
		t.Fatal("retry did not receive the cached generation")
	}
	if string(original) != "data: cached\n\n" || string(cached) != string(original) {
		t.Fatalf("original=%q cached=%q", original, cached)
	}
	again, isLeader := store.begin(key, time.Now())
	if isLeader {
		t.Fatal("a retry inside the cache window started another generation")
	}
	replayed, err := again.follow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	downloaded, err := io.ReadAll(replayed.Body)
	if err != nil || string(downloaded) != string(original) {
		t.Fatalf("downloaded=%q err=%v", downloaded, err)
	}
}

func TestIdleReplayDropsEmptyFailure(t *testing.T) {
	var store idleReplayStore
	key := "empty"
	leader, _ := store.begin(key, time.Now())
	leader.finish()
	if _, isLeader := store.begin(key, time.Now()); !isLeader {
		t.Fatal("an idle timeout with no bytes should not block a later request")
	}
}

func TestContinueReusesCompletedSessionGeneration(t *testing.T) {
	service := &Service{}
	first := Input{
		ClientKey: clientkeydomain.Key{ID: 7}, PublicModel: "grok-4.5", PromptCacheKey: "session-1",
		Body: []byte(`{"input":[{"role":"user","content":"写一篇长文"}]}`), Streaming: true, Operation: audit.OperationResponses,
	}
	leader, isLeader := service.beginIdleReplay(&first)
	if !isLeader {
		t.Fatal("first turn should call the model")
	}
	payload := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"已经写完\"}\n\ndata: {\"type\":\"response.completed\"}\n\n"
	body := leader.tee(http.StatusOK, nil, io.NopCloser(strings.NewReader(payload)))
	if _, err := io.ReadAll(body); err != nil {
		t.Fatal(err)
	}
	_ = body.Close()

	next := Input{
		ClientKey: first.ClientKey, PublicModel: first.PublicModel, PromptCacheKey: first.PromptCacheKey,
		Body: []byte(`{"input":[{"role":"user","content":"写一篇长文"},{"role":"user","content":"继续"}]}`), Streaming: true, Operation: audit.OperationResponses,
	}
	entry, isLeader := service.beginIdleReplay(&next)
	if isLeader || entry != leader {
		t.Fatal("continue should download the finished generation instead of calling the model")
	}
	result, err := entry.follow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	downloaded, err := io.ReadAll(result.Body)
	if err != nil || string(downloaded) != payload {
		t.Fatalf("downloaded=%q err=%v", downloaded, err)
	}
}

func TestContinueSplicesPartialSessionGeneration(t *testing.T) {
	service := &Service{}
	first := Input{
		ClientKey: clientkeydomain.Key{ID: 7}, PublicModel: "grok-4.5", PromptCacheKey: "session-1",
		Body: []byte(`{"messages":[{"role":"user","content":"写一篇"}]}`), Streaming: true, Operation: audit.OperationChat,
	}
	leader, isLeader := service.beginIdleReplay(&first)
	if !isLeader {
		t.Fatal("first turn should call the model")
	}
	payload := "data: {\"choices\":[{\"delta\":{\"content\":\"写到一半\"}}]}\n\n"
	body := leader.tee(http.StatusOK, nil, io.NopCloser(strings.NewReader(payload)))
	if _, err := io.ReadAll(body); err != nil {
		t.Fatal(err)
	}
	_ = body.Close()

	next := Input{
		ClientKey: first.ClientKey, PublicModel: first.PublicModel, PromptCacheKey: first.PromptCacheKey,
		Body:      []byte(`{"messages":[{"role":"user","content":"写一篇"},{"role":"user","content":"continue"}]}`),
		Streaming: true, Operation: audit.OperationChat,
	}
	if _, isLeader = service.beginIdleReplay(&next); !isLeader {
		t.Fatal("a partial answer should continue with one model call, not be replayed as finished")
	}
	var payloadBody struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := jsonUnmarshal(next.Body, &payloadBody); err != nil {
		t.Fatal(err)
	}
	if len(payloadBody.Messages) != 3 || payloadBody.Messages[1].Role != "assistant" || payloadBody.Messages[1].Content != "写到一半" || payloadBody.Messages[2].Content != "continue" {
		t.Fatalf("handoff messages = %#v", payloadBody.Messages)
	}
}

func TestContinuationCueRejectsRealQuestions(t *testing.T) {
	if !isContinuationCue("继续。") || !isContinuationCue("Please continue") {
		t.Fatal("short continue cues should hand off the stalled turn")
	}
	if isContinuationCue("继续把上一节的接口补完并加上测试") {
		t.Fatal("a real follow-up must not be treated as a stalled-turn download")
	}
}

func jsonUnmarshal(data []byte, value any) error {
	return json.Unmarshal(data, value)
}

func TestReplayPrefixKeepsBufferedIdleBytes(t *testing.T) {
	restored, ok := replayPrefix(io.NopCloser(strings.NewReader("data: partial")))
	if !ok {
		t.Fatal("buffered idle payload was dropped")
	}
	data, err := io.ReadAll(restored)
	if err != nil || string(data) != "data: partial" {
		t.Fatalf("data=%q err=%v", data, err)
	}
	if _, ok := replayPrefix(io.NopCloser(strings.NewReader(""))); ok {
		t.Fatal("empty idle payload should not be returned as a cached response")
	}
}
