package safe

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type (
	idFromString string
	idFromStruct struct{}
)

func (idFromString) ID() string {
	return ""
}

func (*idFromStruct) ID() string {
	return ""
}

func TestID(t *testing.T) {
	for _, tt := range []id{
		nil,
		idFromString(""),
		&idFromStruct{},
		(*idFromStruct)(nil),
	} {
		require.NotPanics(t, func() {
			_ = ID(tt)
		})
	}
}
