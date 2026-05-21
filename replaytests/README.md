This sample demonstrates replay testing with the `replaysuite` test wrapper.
It contains a few examples of workflows with generated histories and
instructions on how to change them to fail the replay tests.

Running the suite executes the regular workflow unit tests and, in addition,
mirrors each test against a local dev server to record real event histories
under `.histories/`. On the next run those recorded histories are replayed
against the current workflow code, catching non-deterministic changes if the
code has changed since the history was produced. The local server is fully
managed by the test suite.

Steps to run this sample:

1) Run the test suite (`-count=1` disables Go's test cache so it always
   re-runs):
```
go test -count=1 -v ./...
```

To run a single suite, pass the suite's top-level test name to `-run`:
```
go test -count=1 -v . -run '^TestWorkflowTimerTestSuite$'
```

To run a single test inside a suite, include both the suite test name and the
suite method subtest name:
```
go test -count=1 -v . -run '^TestWorkflowTimerTestSuite$/^Test_WorkflowWithTimer$'
```

Try deleting the `.histories` folder such that the first run has no histories.
In this case it will simply generate the histories for each workflow under
`replaytests/.histories/<workflow_type>/`.

2) Run it again:
```
go test -count=1 -v ./...
```
Use the same `-run` filters from step 1 if you only want to rerun one suite or
one test against its recorded histories.

This time `SetupSuite` replays every history in `.histories/` against each
workflow registered for replay before the unit tests run, then regenerates them.
A passing run means the workflow is still compatible with the previously
recorded histories. If the run fails, the old history files are deleted.

## Seeing a replay failure

1) Run `go test -count=1 -v ./...` once to generate histories.
2) In any test file, find the comment explaining how to change the workflow to
   trigger a failure.
3) Run `go test -count=1 -v ./...` again. The replay step in `SetupSuite` now
   fails with a non-determinism error, because the recorded histories no longer
   match the modified workflow, and the suite stops before any unit test runs.
