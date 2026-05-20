This sample demonstrates replay testing with the `replaysuite` test wrapper.

Running the suite executes the regular workflow unit tests and, in addition,
mirrors each test against a local dev server to record real event histories
under `.histories/`. On the next run those recorded histories are replayed
against the current workflow code, catching non-deterministic changes.

`replaysuite` starts its own dev server, so no separate Temporal server is
needed to run these tests.

Steps to run this sample:

1) Run the test suite (`-count=1` disables Go's test cache so it always
   re-runs):
```
go test -count=1 -v ./...
```
The first run has no histories yet, so it just runs the unit tests and writes
one history file per test into `replaytests/.histories/Workflow/`.

2) Run it again:
```
go test -count=1 -v ./...
```
This time `SetupSuite` replays every history in `.histories/` against
`Workflow` before the unit tests run, then regenerates them. A passing run
means the workflow is still compatible with the previously recorded histories.

## Seeing a replay failure

1) Run `go test -count=1 -v ./...` once to generate histories.
2) In `workflow.go`, uncomment the block marked
   `// Uncomment this to fail the replay test.` — it adds an extra activity
   call, changing the workflow's command sequence.
3) Run `go test -count=1 -v ./...` again. The replay step in `SetupSuite` now
   fails with a non-determinism error, because the recorded histories no longer
   match the modified workflow, and the suite stops before any unit test runs.
