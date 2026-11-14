package replaysuite

import (
	"reflect"
	"runtime"
	"strings"
)

// workflowKey gives a stable identifier for a workflow defined in one Go
// package. The purpose of this identifier is to allow associating a certain
// history record with a particular workflow that we need to replay against.
// Since a go package does not allow having two functions with the same name,
// this will produce a different workflow identifier for each workflow function.
//
// This identifier can be used as a file name.
//
// See https://pkg.go.dev/go.temporal.io/sdk/workflow#hdr-Declaration to
// understand what can be a workflow.
func workflowKey(w any) string {
	name, ok := w.(string)
	if !ok {
		name = strings.TrimSuffix(runtime.FuncForPC(reflect.ValueOf(w).Pointer()).Name(), "-fm")
	}
	return strings.ReplaceAll(name, "/", "_")
}
