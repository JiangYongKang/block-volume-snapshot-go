package blockvol

import "errors"

// 可按类型区分的错误：调用方可用 errors.Is 判断失败类别。
var (
	// ErrVolumeNotFound 卷不存在（可能已删除）。
	ErrVolumeNotFound = errors.New("volume not found")
	// ErrSnapshotNotFound 快照不存在。
	ErrSnapshotNotFound = errors.New("snapshot not found")
	// ErrVolumeExists 同名卷已存在。
	ErrVolumeExists = errors.New("volume already exists")
	// ErrSnapshotExists 同名快照已存在。
	ErrSnapshotExists = errors.New("snapshot already exists")
	// ErrOutOfRange 访问的块号超出卷容量。
	ErrOutOfRange = errors.New("block index out of range")
	// ErrInvalidArgument 参数非法（如块大小、数据长度）。
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrClosed 存储已关闭。
	ErrClosed = errors.New("store closed")
	// ErrLocked 存储已被其他进程以独占方式打开。
	ErrLocked = errors.New("store is locked by another process")
)
