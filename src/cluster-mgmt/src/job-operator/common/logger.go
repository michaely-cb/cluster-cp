package common

import (
	"flag"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	zaplogfmt "github.com/sykesm/zap-logfmt"
	uzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

var (
	levelFlag = flag.String("log-level", "info", "debug|info|warn|error")
)

type Logger struct {
	z *uzap.Logger
	l logr.Logger
}

var (
	Log      *Logger
	initOnce sync.Once
)

func InitLogger() *Logger {
	initOnce.Do(func() {
		var zapOpts crzap.Options
		zapOpts.BindFlags(flag.CommandLine)
		flag.Parse()

		configLog := uzap.NewProductionEncoderConfig()
		configLog.EncodeTime = func(ts time.Time, encoder zapcore.PrimitiveArrayEncoder) {
			encoder.AppendString(ts.UTC().Format(time.RFC3339Nano))
		}
		enc := zaplogfmt.NewEncoder(configLog)

		lvl := uzap.InfoLevel
		switch strings.ToLower(*levelFlag) {
		case "debug":
			lvl = uzap.DebugLevel
		case "warn":
			lvl = uzap.WarnLevel
		case "error":
			lvl = uzap.ErrorLevel
		}

		core := zapcore.NewCore(enc, zapcore.AddSync(os.Stdout), lvl)
		z := uzap.New(core, uzap.AddCaller(), uzap.AddCallerSkip(1))
		Log = &Logger{z: z, l: zapr.NewLogger(z)}
	})
	return Log
}

func (lg *Logger) Logr() logr.Logger { return lg.l }

func (lg *Logger) WithField(k string, v interface{}) *Logger {
	z2 := lg.z.With(uzap.Any(k, v))
	return &Logger{z: z2, l: lg.l.WithValues(k, v)}
}

func (lg *Logger) WithFields(m map[string]interface{}) *Logger {
	fields := make([]uzap.Field, 0, len(m))
	kvs := make([]interface{}, 0, len(m)*2)
	for k, v := range m {
		fields = append(fields, uzap.Any(k, v))
		kvs = append(kvs, k, v)
	}
	z2 := lg.z.With(fields...)
	return &Logger{z: z2, l: lg.l.WithValues(kvs...)}
}

func (lg *Logger) Debug(args ...interface{}) {
	lg.z.Sugar().Debug(args...)
}

func (lg *Logger) Debugf(format string, args ...interface{}) {
	lg.z.Sugar().Debugf(format, args...)
}

func (lg *Logger) Info(args ...interface{}) {
	lg.z.Sugar().Info(args...)
}

func (lg *Logger) Infof(format string, args ...interface{}) {
	lg.z.Sugar().Infof(format, args...)
}

func (lg *Logger) Warn(args ...interface{}) {
	lg.z.Sugar().Warn(args...)
}

func (lg *Logger) Warnf(format string, args ...interface{}) {
	lg.z.Sugar().Warnf(format, args...)
}

func (lg *Logger) Error(args ...interface{}) {
	lg.z.Sugar().Error(args...)
}

func (lg *Logger) Errorf(format string, args ...interface{}) {
	lg.z.Sugar().Errorf(format, args...)
}

func init() {
	// Initialize with basic defaults for backward compatibility
	// This will be overridden if Init() is called explicitly
	config := uzap.NewDevelopmentConfig()
	config.Level = uzap.NewAtomicLevelAt(zapcore.DebugLevel)
	zapLogger, err := config.Build(uzap.AddCallerSkip(1))
	if err != nil {
		zapLogger = uzap.NewNop()
	}
	Log = &Logger{z: zapLogger, l: zapr.NewLogger(zapLogger)}
}
