//go:build windows

package jsruntime

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type nodeRuntime struct {
	registry registry

	cmd   *exec.Cmd
	stdin io.WriteCloser

	pendingMu sync.Mutex
	pending   map[int64]chan nodeMessage

	writeMu sync.Mutex
	seq     int64

	stderr    *lockedBuffer
	doneCh    chan struct{}
	waitMu    sync.RWMutex
	waitErr   error
	closeOnce sync.Once
	tempPath  string
}

type nodeMessage struct {
	Type     string       `json:"type"`
	ID       int64        `json:"id,omitempty"`
	Name     string       `json:"name,omitempty"`
	Filename string       `json:"filename,omitempty"`
	Source   string       `json:"source,omitempty"`
	Args     []any        `json:"args,omitempty"`
	Result   any          `json:"result"`
	Error    *remoteError `json:"error,omitempty"`
}

type remoteError struct {
	Message string `json:"message"`
	Stack   string `json:"stack,omitempty"`
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newNodeRuntime() (Runtime, error) {
	file, err := os.CreateTemp("", "go-future-js-*.js")
	if err != nil {
		return nil, fmt.Errorf("create node runner: %w", err)
	}
	defer file.Close()

	if _, err := file.WriteString(nodeRunnerSource); err != nil {
		_ = os.Remove(file.Name())
		return nil, fmt.Errorf("write node runner: %w", err)
	}

	cmd := exec.Command("node", file.Name())
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = os.Remove(file.Name())
		return nil, fmt.Errorf("open node stdout: %w", err)
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = os.Remove(file.Name())
		return nil, fmt.Errorf("open node stdin: %w", err)
	}

	stderr := &lockedBuffer{}
	cmd.Stderr = stderr

	rt := &nodeRuntime{
		registry: newRegistry(),
		cmd:      cmd,
		stdin:    stdin,
		pending:  make(map[int64]chan nodeMessage),
		stderr:   stderr,
		doneCh:   make(chan struct{}),
		tempPath: file.Name(),
	}

	if err := cmd.Start(); err != nil {
		_ = os.Remove(file.Name())
		return nil, fmt.Errorf("start node runner: %w", err)
	}

	go rt.readLoop(stdout)
	go rt.waitLoop()

	if err := rt.LoadScript("__bridge_prelude__.js", bridgePreludeSource); err != nil {
		_ = rt.Close()
		return nil, err
	}

	return rt, nil
}

func (rt *nodeRuntime) Backend() string {
	return "node"
}

func (rt *nodeRuntime) Register(name string, fn HostFunc) error {
	return rt.registry.Register(name, fn)
}

func (rt *nodeRuntime) LoadScript(filename, source string) error {
	if filename == "" {
		filename = "inline.js"
	}

	_, err := rt.sendCommand(nodeMessage{
		Type:     "load",
		Filename: filename,
		Source:   source,
	})
	return err
}

func (rt *nodeRuntime) Call(name string, args ...any) (any, error) {
	normalizedArgs, err := normalizeJSONArgs(args)
	if err != nil {
		return nil, err
	}

	return rt.sendCommand(nodeMessage{
		Type: "call",
		Name: name,
		Args: normalizedArgs,
	})
}

func (rt *nodeRuntime) Close() error {
	var closeErr error

	rt.closeOnce.Do(func() {
		if !rt.isDone() {
			_, _ = rt.sendCommand(nodeMessage{Type: "close"})

			select {
			case <-rt.doneCh:
			case <-time.After(2 * time.Second):
				if rt.cmd != nil && rt.cmd.Process != nil {
					_ = rt.cmd.Process.Kill()
				}
				<-rt.doneCh
			}
		}

		if rt.stdin != nil {
			if err := rt.stdin.Close(); err != nil && closeErr == nil && !errors.Is(err, os.ErrClosed) {
				closeErr = err
			}
		}

		if rt.tempPath != "" {
			if err := os.Remove(rt.tempPath); err != nil && closeErr == nil && !errors.Is(err, os.ErrNotExist) {
				closeErr = err
			}
		}
	})

	return closeErr
}

func (rt *nodeRuntime) waitLoop() {
	err := rt.cmd.Wait()
	rt.setWaitErr(err)
	close(rt.doneCh)
	rt.failPending(rt.processError("node bridge exited"))
}

func (rt *nodeRuntime) readLoop(reader io.Reader) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()

		var msg nodeMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			rt.failPending(fmt.Errorf("failed to decode node bridge message: %w", err))
			return
		}

		switch msg.Type {
		case "host_call":
			go rt.handleHostCall(msg)
		case "response":
			rt.deliverResponse(msg)
		}
	}

	if err := scanner.Err(); err != nil {
		rt.failPending(fmt.Errorf("failed reading node bridge output: %w", err))
	}
}

func (rt *nodeRuntime) handleHostCall(msg nodeMessage) {
	result, err := rt.registry.Invoke(msg.Name, msg.Args)

	response := nodeMessage{
		Type:   "host_response",
		ID:     msg.ID,
		Result: result,
	}
	if err != nil {
		response.Error = &remoteError{Message: err.Error()}
	}

	if writeErr := rt.writeJSON(response); writeErr != nil {
		_, _ = rt.stderr.Write([]byte(writeErr.Error() + "\n"))
	}
}

func (rt *nodeRuntime) sendCommand(message nodeMessage) (any, error) {
	if rt.isDone() {
		return nil, rt.processError("node bridge is not running")
	}

	id := atomic.AddInt64(&rt.seq, 1)
	message.ID = id

	responseCh := make(chan nodeMessage, 1)
	rt.pendingMu.Lock()
	rt.pending[id] = responseCh
	rt.pendingMu.Unlock()

	if err := rt.writeJSON(message); err != nil {
		rt.removePending(id)
		return nil, err
	}

	select {
	case response := <-responseCh:
		if response.Error != nil {
			return nil, response.Error.asError("node bridge command failed")
		}
		return response.Result, nil
	case <-rt.doneCh:
		rt.removePending(id)
		return nil, rt.processError("node bridge stopped while waiting for a response")
	}
}

func (rt *nodeRuntime) writeJSON(message nodeMessage) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal node bridge message: %w", err)
	}

	rt.writeMu.Lock()
	defer rt.writeMu.Unlock()

	if _, err := rt.stdin.Write(append(payload, '\n')); err != nil {
		return fmt.Errorf("write node bridge message: %w", err)
	}

	return nil
}

func (rt *nodeRuntime) deliverResponse(msg nodeMessage) {
	rt.pendingMu.Lock()
	ch := rt.pending[msg.ID]
	delete(rt.pending, msg.ID)
	rt.pendingMu.Unlock()

	if ch != nil {
		ch <- msg
	}
}

func (rt *nodeRuntime) failPending(err error) {
	rt.pendingMu.Lock()
	defer rt.pendingMu.Unlock()

	for id, ch := range rt.pending {
		ch <- nodeMessage{
			Type:   "response",
			ID:     id,
			Result: nil,
			Error:  &remoteError{Message: err.Error()},
		}
		delete(rt.pending, id)
	}
}

func (rt *nodeRuntime) removePending(id int64) {
	rt.pendingMu.Lock()
	defer rt.pendingMu.Unlock()
	delete(rt.pending, id)
}

func (rt *nodeRuntime) isDone() bool {
	select {
	case <-rt.doneCh:
		return true
	default:
		return false
	}
}

func (rt *nodeRuntime) setWaitErr(err error) {
	rt.waitMu.Lock()
	defer rt.waitMu.Unlock()
	rt.waitErr = err
}

func (rt *nodeRuntime) getWaitErr() error {
	rt.waitMu.RLock()
	defer rt.waitMu.RUnlock()
	return rt.waitErr
}

func (rt *nodeRuntime) processError(prefix string) error {
	var details []string
	details = append(details, prefix)

	if err := rt.getWaitErr(); err != nil {
		details = append(details, err.Error())
	}

	if stderr := strings.TrimSpace(rt.stderr.String()); stderr != "" {
		details = append(details, "node stderr:\n"+stderr)
	}

	return errors.New(strings.Join(details, "\n"))
}

func (e *remoteError) asError(prefix string) error {
	if e == nil {
		return nil
	}

	if e.Stack != "" {
		return fmt.Errorf("%s: %s\n%s", prefix, e.Message, e.Stack)
	}

	return fmt.Errorf("%s: %s", prefix, e.Message)
}
