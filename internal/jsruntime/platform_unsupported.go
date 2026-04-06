//go:build !windows

package jsruntime

import "fmt"

func newPlatformRuntime() (Runtime, error) {
	return nil, fmt.Errorf("javascript runtime is currently only configured for windows/node")
}
