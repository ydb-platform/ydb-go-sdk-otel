package safe

import (
	"fmt"
	"reflect"
	"strconv"
)

func Stringer(s fmt.Stringer) string {
	if s == nil {
		return ""
	}
	return s.String()
}

type resultErr interface {
	Err() error
}

func Err(r resultErr) error {
	if r == nil {
		return nil
	}
	return r.Err()
}

func Error(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type address interface {
	Address() string
}

func Address(a address) string {
	if a == nil {
		return ""
	}
	return a.Address()
}

type nodeID interface {
	NodeID() uint32
}

func NodeID(n nodeID) string {
	if n == nil {
		return "0"
	}
	return strconv.FormatUint(uint64(n.NodeID()), 10)
}

type id interface {
	ID() string
}

func ID(id id) string {
	v := reflect.ValueOf(id)
	if v.IsNil() {
		return ""
	}
	return id.ID()
}

type status interface {
	Status() string
}

func Status(s status) string {
	if s == nil {
		return ""
	}
	return s.Status()
}
