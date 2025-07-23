package common

import (
	"context"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
)

var (
	InfoLogger  *ContextLogger
	ErrorLogger *ContextLogger
)

type ContextLogger struct {
	*zap.Logger
}

func (l *ContextLogger) WithContext(ctx context.Context) *zap.Logger {
	if requestID := ctx.Value("X-Request-Id"); requestID != nil {
		return l.Logger.With(zap.String("request_id", requestID.(string)))
	}
	return l.Logger
}

func SetLog(infoLogger, errorLogger *zap.Logger) {
	InfoLogger = &ContextLogger{infoLogger}
	ErrorLogger = &ContextLogger{errorLogger}
}

func InitLog(infoLog, errorLog string, LogLevel int64) {
	// 编码器配置
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:      "time",
		LevelKey:     "level",
		MessageKey:   "msg",
		CallerKey:    "caller",
		EncodeLevel:  zapcore.LowercaseLevelEncoder,
		EncodeTime:   zapcore.ISO8601TimeEncoder,
		EncodeCaller: zapcore.ShortCallerEncoder,
	}

	// INFO级别日志配置
	infoWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   infoLog,
		MaxSize:    512,
		MaxBackups: 5,
		MaxAge:     7,
		Compress:   false,
	})

	// ERROR级别日志配置
	errorWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   errorLog,
		MaxSize:    128,
		MaxBackups: 7,
		MaxAge:     30,
		Compress:   false,
	})

	// 创建核心
	infoCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.NewMultiWriteSyncer(infoWriter, zapcore.AddSync(os.Stdout)),
		zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
			return lvl >= LogLevelMap[LogLevel]
		}),
	)

	errorCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.NewMultiWriteSyncer(errorWriter),
		zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
			return lvl >= zap.WarnLevel // 仅记录Warn及以上级别
		}),
	)

	// 构建Logger
	infoLogger := zap.New(infoCore, zap.AddCaller())
	errorLogger := zap.New(
		errorCore, zap.AddCaller(),
		zap.AddStacktrace(zap.ErrorLevel))

	InfoLogger = &ContextLogger{infoLogger}
	ErrorLogger = &ContextLogger{errorLogger}
}
