package fdb

import (
	"errors"
	"testing"

	"golang.org/x/xerrors"
)

type futureNilValue struct {
	err error
}

func (f futureNilValue) Get() error {
	return f.err
}

func (f futureNilValue) MustGet() {
	if f.err != nil {
		panic(f.err)
	}
}

func (f futureNilValue) BlockUntilReady() {}

func (f futureNilValue) IsReady() bool { return true }

func (f futureNilValue) Cancel() {}

func TestRetryable(t *testing.T) {
	retryableError := Error{1007}
	wrapped := func() (interface{}, error) {
		return nil, retryableError
	}

	var ep Error
	onError := func(e Error) FutureNil {
		ep = e
		return futureNilValue{errors.New("stop")}
	}

	retryable(wrapped, onError)

	if ep != retryableError {
		t.Errorf("onError not called with fdb.Error")
	}
}

func TestRetryableWrap(t *testing.T) {
	retryableError := Error{1007}
	wrappedError := xerrors.Errorf("failed to read key: %w", retryableError)
	wrapped := func() (interface{}, error) {
		return nil, wrappedError
	}

	var ep Error
	onError := func(e Error) FutureNil {
		ep = e
		return futureNilValue{errors.New("stop")}
	}

	retryable(wrapped, onError)

	if ep.Error() != retryableError.Error() {
		t.Errorf("onError not called with fdb.Error, but %T: %v", ep, ep)
	}
}
