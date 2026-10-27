package replaysuite

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"
)

type MockCallWrapper struct {
	call  *mock.Call
	inner *testsuite.MockCallWrapper
}

func (m *MockCallWrapper) Return(returnArguments ...any) *MockCallWrapper {
	m.call.Return(returnArguments...)
	m.inner.Return(returnArguments...)
	return m
}

func (m *MockCallWrapper) Once() *MockCallWrapper {
	return m.Times(1)
}

func (m *MockCallWrapper) Twice() *MockCallWrapper {
	return m.Times(2)
}

func (m *MockCallWrapper) Times(i int) *MockCallWrapper {
	m.call.Times(i)
	m.inner.Times(i)
	return m
}

func (m *MockCallWrapper) Never() *MockCallWrapper {
	// replicate sdk implementation
	m.call.Maybe()
	m.call.Panic(fmt.Sprintf("unexpected call: %s(%s)", m.call.Method, m.call.Arguments.String()))
	m.inner.Never()
	return m
}

func (m *MockCallWrapper) Maybe() *MockCallWrapper {
	panic("unsupported yet")
}

func (m *MockCallWrapper) Run(fn func(args mock.Arguments)) *MockCallWrapper {
	panic("unsupported yet")
}

func (m *MockCallWrapper) After(d time.Duration) *MockCallWrapper {
	panic("unsupported yet")
}

func (m *MockCallWrapper) AfterFn(fn func() time.Duration) *MockCallWrapper {
	panic("unsupported yet")
}

func (m *MockCallWrapper) Panic(msg string) *MockCallWrapper {
	panic("unsupported yet")
}

func (m *MockCallWrapper) NotBefore(calls ...*MockCallWrapper) *MockCallWrapper {
	panic("unsupported yet")
}
