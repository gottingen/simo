// +build windows

package http

import "testing"

func TestURIPathNormalizeIssue86(t *testing.T) {
	t.Parallel()

	var u URI

	testURIPathNormalize(t, &u, `C:\a\b\c\fs.go`, `C:\a\b\c\fs.go`)
}
