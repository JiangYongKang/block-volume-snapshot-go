package blockvol

import (
	"io"
	"log"
	"os"
	"time"
)

const (
	// 默认块大小（字节），一个逻辑块是快照/写时复制的最小单位。
	defaultBlockSize = 4096
	// 默认后台回收间隔。
	defaultGCInterval = time.Second
)

// Logger 接收存储内部关键操作日志（创建/删除/快照/回收/引用计数变化）。
// 与标准库 *log.Logger 兼容；测试可传入打印到 testing 日志的实现。
type Logger interface {
	Printf(format string, args ...any)
}

// discardLogger 是默认日志器：什么都不输出。
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Options 控制打开存储时的行为。
type Options struct {
	// BlockSize 逻辑块大小（字节），新建存储时生效；已存在的存储以元数据为准。
	BlockSize int
	// GCInterval 后台回收间隔，<=0 表示不启动后台回收（仍可手动 RunGC）。
	GCInterval time.Duration
	// Logger 关键操作日志出口；nil 表示丢弃。
	Logger Logger
}

// Option 是 Open 的函数式选项。
type Option func(*Options)

// WithBlockSize 设置逻辑块大小。
func WithBlockSize(n int) Option { return func(o *Options) { o.BlockSize = n } }

// WithGCInterval 设置后台 GC 间隔；传 0 关闭后台 GC。
func WithGCInterval(d time.Duration) Option { return func(o *Options) { o.GCInterval = d } }

// WithLogger 设置操作日志出口（例如 log.New(os.Stdout, ...)）。
func WithLogger(l Logger) Option { return func(o *Options) { o.Logger = l } }

// StdLogger 返回输出到 stdout 的标准库 logger，便于直接传入 WithLogger。
func StdLogger() Logger { return log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds) }

// DiscardLogger 返回丢弃所有日志的 logger。
func DiscardLogger() Logger { return discardLogger{} }

// LoggerWriter 把任意 io.Writer 包装成 Logger。
func LoggerWriter(w io.Writer) Logger { return log.New(w, "", log.LstdFlags|log.Lmicroseconds) }

// GCStats 描述一次垃圾回收的结果，便于测试与排障。
type GCStats struct {
	// Scanned 扫描到的物理块文件数。
	Scanned int
	// Reclaimed 被删除的无引用块数。
	Reclaimed int
	// LiveBlocks 当前仍被引用的块数。
	LiveBlocks int
}
