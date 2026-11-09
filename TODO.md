Featuers I want to cover:

- finish up suite wrapper
- continue as new
- workflow task failures e.g. panic
- signals, queries
- dynamic handlers
- nexus
- side effect and mutable side effect
- figure out cases where the execution on dev server fails but doesn't stop test
execution
- figure out a better way to handle workflow tasks in between child workflow
executions in test
- if a workflow is defined twice with the same name in different packages it may be difficult to figure out what you actually need to replay. Should probably add an identifier to the test suite that will be used in the path that can later be used by the replayer. I don't have a good design for this yet.