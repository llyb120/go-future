package jsruntime

import (
	"encoding/json"
	"fmt"
	"sync"
)

type HostFunc func(args []any) (any, error)

type Runtime interface {
	Backend() string
	Register(name string, fn HostFunc) error
	LoadScript(filename, source string) error
	Call(name string, args ...any) (any, error)
	Close() error
}

func New() (Runtime, error) {
	return newPlatformRuntime()
}

type registry struct {
	mu    sync.RWMutex
	funcs map[string]HostFunc
}

func newRegistry() registry {
	return registry{
		funcs: make(map[string]HostFunc),
	}
}

func (r *registry) Register(name string, fn HostFunc) error {
	if name == "" {
		return fmt.Errorf("host function name must not be empty")
	}
	if fn == nil {
		return fmt.Errorf("host function %q is nil", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.funcs[name]; exists {
		return fmt.Errorf("host function %q is already registered", name)
	}

	r.funcs[name] = fn
	return nil
}

func (r *registry) Invoke(name string, args []any) (any, error) {
	r.mu.RLock()
	fn, ok := r.funcs[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("host function %q was not registered", name)
	}

	normalizedArgs, err := normalizeJSONArgs(args)
	if err != nil {
		return nil, err
	}

	result, err := fn(normalizedArgs)
	if err != nil {
		return nil, err
	}

	return normalizeJSONValue(result)
}

func normalizeJSONArgs(args []any) ([]any, error) {
	normalized := make([]any, len(args))
	for i, arg := range args {
		value, err := normalizeJSONValue(arg)
		if err != nil {
			return nil, fmt.Errorf("argument %d: %w", i, err)
		}
		normalized[i] = value
	}
	return normalized, nil
}

func normalizeJSONValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("value must be JSON-compatible: %w", err)
	}

	var normalized any
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return nil, fmt.Errorf("failed to normalize value: %w", err)
	}

	return normalized, nil
}
