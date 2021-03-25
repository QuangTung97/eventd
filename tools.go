// +build tools

package tools

import (
	_ "github.com/fzipp/gocyclo"
	_ "github.com/golang/mock/mockgen"
	_ "github.com/kisielk/errcheck"
	_ "golang.org/x/lint/golint"
)
