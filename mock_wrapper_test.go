package replaysuite

import (
	"reflect"
	"slices"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

func publicMethodNames(t reflect.Type) []string {
	names := make([]string, 0, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		names = append(names, t.Method(i).Name)
	}
	return names
}

func TestMockCallWrapperMatchesInnerMethods(t *testing.T) {
	// Given
	wrapperType := reflect.TypeFor[*MockCallWrapper]()
	innerType := reflect.TypeFor[*testsuite.MockCallWrapper]()

	// When
	wrapperMethods := publicMethodNames(wrapperType)
	innerMethods := publicMethodNames(innerType)

	// Then
	if !slices.Equal(wrapperMethods, innerMethods) {
		t.Fatalf("methods = %v, want %v", wrapperMethods, innerMethods)
	}
}
