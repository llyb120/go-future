package jsruntime

import _ "embed"

var (
	//go:embed bridge_prelude.js
	bridgePreludeSource string

	//go:embed node_runner.js
	nodeRunnerSource string
)
