# AGENTS.md

replaysuite wraps the Temporal Go SDK `TestWorkflowEnvironment`. Every workflow
executed in a unit test is also mirrored on a real Temporal server, so that its
event history can be written to disk.

It aims to be a drop-in replacement: existing tests should keep working
unchanged and behave the same. In some cases that isn't possible. Those are
listed under Limitations in the README.

## Layout

- `env.go`: the `TestWorkflowEnvironment` wrapper and the mirrored executions.
- `interceptor_mock.go`: serves `OnActivity` mocks on the mirror worker.
- `interceptor_fast.go`: shortens timers and retries on the mirror worker.
- `regulartests/`: tests against the plain SDK test environment only. They
  document how the SDK behaves, and replaysuite must match that behavior.

## How it works

- `ExecuteWorkflow` first runs the workflow in the regular SDK test
  environment. It then starts the same workflow through a real worker on the
  Temporal server and waits for it to finish.
- The environment then fetches the full event history of that execution and
  writes it as JSON to `.histories/<workflow type>/<workflow id>.json`.
  Payloads are base64 encoded in the history, so each one is also annotated
  with its decoded value to make the files easier to read.
- Registrations and mocks are applied to both. Activities registered on the
  environment are also registered on the worker, and `OnActivity` sets up the
  same expectation on both the SDK mock and the wrapper's own mock.
- The worker runs two interceptors. The mock interceptor answers mocked
  activities from the wrapper's mock and lets the rest run their real
  implementation. The fast interceptor keeps activities on the worker's task
  queue and shortens timers and retries, so the mirrored run finishes quickly.

## Testing

```sh
go test ./...                                         # dev server started by TestMain
REPLAYSUITE_TEST_WITH_REAL_SERVER=true go test ./...  # server on localhost:7233
go test -parallel 1 ./...                             # run tests sequentially
go test -timeout 60s ./...                            # dump goroutines if it hangs
```

Running against the local server on `localhost:7233` is fine.

## Project guidelines

- Mirror the SDK behavior as closely as possible. If the SDK panics in some
  scenario, the wrapper should panic too. If the SDK handles something in a
  particular way, such as aliasing, the wrapper should handle it the same way.
- Before changing the wrapper, read how the SDK implements the same thing and
  take it as a starting point. The SDK test environment lives in `internal/` of
  `go.temporal.io/sdk` (`go list -m -f '{{.Dir}}' go.temporal.io/sdk`), mainly
  `workflow_testsuite.go`, `internal_workflow_testsuite.go` and
  `internal_worker.go`.
- When unsure how the SDK behaves, write a test in `regulartests/` to confirm
  it first, then match it.
- For new behavior, write the failing tests first and stop. Implement only
  once asked to.
- Only change what was asked for. Don't edit `TestMain` or restructure existing
  tests without asking.
- When the request is a question or a design discussion, answer it; don't edit
  files.
- Keep doc comments high-level.
- Only edit the README when explicitly asked to, and follow those instructions.
  Otherwise, suggest README changes instead of making them.
- In shell commands, use absolute paths instead of relative globs like `*.go`.
  A command that exits with no output succeeded; don't re-run it.

## General preferences

- Don't add code comments by default. Only add a comment when I explicitly ask for one, or when the code makes a very specific, non-obvious choice that wouldn't be clear from reading it. In particular, never write comments that justify why one tool, library, or approach was chosen over another (unless I explicitly ask). When you think such comment would be valuable, propose adding it and wait for confirmation.

- When a comment is warranted, keep it short and write it in plain natural language. Avoid AI-sounding phrasing like "this is the crux", "the real blocker", "what actually matters", or constructions like "doing this, never that" — those belong in the trash, not in code or prose. A comment should convey intent, or for genuinely complex pieces, a high-level overview. Don't write comments that narrate history (e.g. "increased x from y", "previously this did z") — that narrative belongs in the PR description if it's useful for review, not in the code. 

- PR descriptions should be concise and describe the change itself, not the process of discovering or debugging it. Don't narrate how the issue was found, what was tried, or dead ends along the way.

- When presenting findings, analysis, or review results, lead directly with the result. Do not prefix with framing sentences like "Here's the crux", "Here's the real issue", "The key finding is", etc. Just state the finding.
