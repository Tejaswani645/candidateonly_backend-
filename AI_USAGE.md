# AI Usage Disclosure (`AI_USAGE.md`)

## 1. Tools Used
- **Antigravity AI Assistant (Google DeepMind)** with Gemini models.
- Standard Go documentation and standard library reference (`net/http`, `sync/atomic`, `encoding/json`).

---

## 2. What Tools Were Used For
- **Architectural Brainstorming**: Designing the streaming batch ingestion pattern for `POST /events` to handle partial payload failures.
- **Code Generation & Boilerplate**: Generating starter HTTP route handlers, store methods, and unit tests in Go.
- **Debugging Verification**: Formulating test cases to verify the 4 bugs fixed in `debugging/main.go`.
- **Drafting Written Sections**: Organizing clear, structured responses for the Scale Memo (Part 4) and Angry Marketer scenario (Part 5).

---

## 3. Suggestion Rejected or Changed & Why
- **AI Suggestion**: The AI initially suggested decoding the entire HTTP `POST /events` body directly into `[]Event` using `json.Unmarshal(body, &events)` and returning a `400 Bad Request` if unmarshaling failed.
- **Reason for Rejection**: The assignment brief specifically warned against decoding the whole request body straight into `[]Event` because a single malformed item would cause the entire batch of 200 events to fail. In real webhook deliveries, providers would repeatedly resend the whole batch. I replaced this with an element-by-element streaming decoder (`json.NewDecoder`) reading `json.RawMessage`, allowing valid items to be ingested while recording specific rejected items in the response payload.

---

## 4. One Thing AI Helped Understand
- **Worker Pool Data Race Nuance**: AI helped clarify how non-atomic primitive increments (`cs.Sent++`) in Go worker goroutines trigger data races when multiple goroutines execute `apply(ev)` concurrently on the same struct instance pointer, even when the enclosing map key lookups are pre-created. Using `atomic.AddInt64` resolved the concurrent mutation race cleanly without needing heavyweight mutexes.

---

## 5. Anything AI Generated That Needed Debugging
- **Test Assertion Count Mismatch**: AI generated an integration test assertion expecting `238` total events from `starter/seed/events.json` based on the file's text line count. When running `go test`, the test failed because `events.json` actually contains `235` JSON objects (due to array brackets `[` and `]` occupying separate lines). I debugged the JSON array length using Python's `json.load()` and updated the test assertion to expect `235`.
