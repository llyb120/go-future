//go:build windows

package jsruntime

func newPlatformRuntime() (Runtime, error) {
	return newNodeRuntime()
}
