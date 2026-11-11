package replaysuite

import (
	"reflect"
	"runtime"
	"strings"
)

// workflowKey gives a stable identifier for a workflow defined in one Go
// package. This identifier can be used as a file name.
//
// See https://pkg.go.dev/go.temporal.io/sdk/workflow#hdr-Declaration to
// understand what can be a workflow.
func workflowKey(w any) string {
	fullName := runtime.FuncForPC(reflect.ValueOf(w).Pointer()).Name()
	return strings.ReplaceAll(strings.TrimSuffix(fullName, "-fm"), "/", "_")
}
