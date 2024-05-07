package ydb

import (
	"errors"
	"io"
)

func skipEOF(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
