package blocksnap

import "errors"

// 错误类型，供调用方按类型区分失败原因。
var (
	ErrNotFound      = errors.New("blocksnap: not found")
	ErrAlreadyExists = errors.New("blocksnap: already exists")
	ErrConflict      = errors.New("blocksnap: operation conflict")
	ErrCorrupt       = errors.New("blocksnap: storage corrupt")
	ErrClosed        = errors.New("blocksnap: store closed")
	ErrInvalidArg    = errors.New("blocksnap: invalid argument")
)
