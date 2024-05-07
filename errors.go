package ydb

import (
	"errors"
	"io"
)

func skipEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
