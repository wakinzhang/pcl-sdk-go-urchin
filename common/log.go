package common

import (
	"context"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
)

type RequestIDHook struct{}

func (h *RequestIDHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *RequestIDHook) Fire(entry *logrus.Entry) error {
	if ctx, ok := entry.Context.(context.Context); ok {
		if requestID := ctx.Value("X-Request-Id"); requestID != nil {
			entry.Data["X-Request-Id"] = requestID
		}
	}
	return nil
}

/*
type writerHook struct {
	Writer    *lumberjack.Logger
	LogLevels []logrus.Level
}

func (hook *writerHook) Fire(entry *logrus.Entry) error {
	line, err := entry.String()
	if err != nil {
		return err
	}
	_, err = hook.Writer.Write([]byte(line))
	return err
}

func (hook *writerHook) Levels() []logrus.Level {
	return hook.LogLevels
}
*/

type AsyncLogger struct {
	ch        chan []byte
	logLevels []logrus.Level
	writer    *lumberjack.Logger
}

func (al *AsyncLogger) Fire(entry *logrus.Entry) error {
	line, err := entry.String()
	if err != nil {
		return err
	}
	al.ch <- []byte(line)
	return err
}

/*
func NewAsyncLogger(bufferSize int) *AsyncLogger {
	al := &AsyncLogger{
		ch:     make(chan []byte, bufferSize),
		writer: &lumberjack.Logger{Filename: "app.log"},
	}
	go al.process()
	return al
}
*/

func (al *AsyncLogger) process() {
	for msg := range al.ch {
		_, _ = al.writer.Write(msg)
	}
}

func (al *AsyncLogger) Levels() []logrus.Level {
	return al.logLevels
}

var Logger = logrus.New()

// InitLog 日志初始化
func InitLog(accessLog, errorLog string, LogLevel int64) {
	// 设置日志Hook
	Logger.AddHook(&RequestIDHook{})

	// 设置日志输出Text格式
	Logger.SetFormatter(&logrus.TextFormatter{
		TimestampFormat:           "2006-01-02 15:04:05.000",
		FullTimestamp:             true,
		ForceColors:               false,
		DisableQuote:              true,
		EnvironmentOverrideColors: true,
		DisableLevelTruncation:    true,
		QuoteEmptyFields:          true,
		FieldMap: logrus.FieldMap{
			"X-Request-Id": "X-Request-Id",
		},
	})
	// 启用调用者信息记录
	Logger.SetReportCaller(true)
	// 设置日志级别输出
	setLevelOutput(logrus.TraceLevel, accessLog, 512)
	setLevelOutput(logrus.ErrorLevel, errorLog, 128)
	// 设置日志记录级别
	Logger.SetLevel(LogLevelMap[LogLevel])
	// 禁用默认终端输出
	Logger.SetOutput(io.Discard)
}

func setLevelOutput(level logrus.Level, filename string, filesize int) {
	hook := &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    filesize, // MB
		MaxBackups: 3,
		MaxAge:     30, // days
		Compress:   true,
	}

	var levels []logrus.Level
	for _, l := range logrus.AllLevels {
		if l <= level { // 包含level及更高级别
			levels = append(levels, l)
		}
	}

	al := &AsyncLogger{
		ch:        make(chan []byte, 4096),
		writer:    hook,
		logLevels: levels,
	}
	go al.process()
	Logger.AddHook(al)

	/*
		Logger.AddHook(&writerHook{
			Writer:    hook,
			LogLevels: levels,
		})
	*/
}
