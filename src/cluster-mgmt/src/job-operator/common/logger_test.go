package common

import (
	"github.com/go-logr/zapr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestLoggerInitialization(t *testing.T) {
	// Reset the global state for testing
	initOnce = sync.Once{}
	logger := InitLogger()
	require.NotNil(t, logger, "InitLogger() should not return nil")
	assert.NotNil(t, logger.z, "Logger should have zap component")
	assert.NotNil(t, logger.l, "Logger should have logr component")
}

func TestLoggerLevels(t *testing.T) {
	tests := []struct {
		level    string
		expected zapcore.Level
	}{
		{"debug", zapcore.DebugLevel},
		{"info", zapcore.InfoLevel},
		{"warn", zapcore.WarnLevel},
		{"error", zapcore.ErrorLevel},
		{"invalid", zapcore.InfoLevel}, // should default to info
	}

	for _, test := range tests {
		t.Run(test.level, func(t *testing.T) {
			core, logs := observer.New(test.expected)
			zapLogger := zap.New(core)
			logger := &Logger{
				z: zapLogger,
				l: zapr.NewLogger(zapLogger),
			}

			// Test all logging levels
			logger.Debug("debug message")
			logger.Info("info message")
			logger.Warn("warn message")
			logger.Error("error message")

			// Verify logs were captured according to level
			allLogs := logs.All()

			// Count expected logs based on level
			expectedCount := 0
			if test.expected <= zapcore.DebugLevel {
				expectedCount = 4 // all logs
			} else if test.expected <= zapcore.InfoLevel {
				expectedCount = 3 // info, warn, error
			} else if test.expected <= zapcore.WarnLevel {
				expectedCount = 2 // warn, error
			} else {
				expectedCount = 1 // error only
			}

			assert.Len(t, allLogs, expectedCount, "Unexpected number of logs for level %s", test.level)
		})
	}
}

func TestLoggerFormatting(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	zapLogger := zap.New(core)
	logger := &Logger{
		z: zapLogger,
		l: zapr.NewLogger(zapLogger),
	}

	// Test formatted logging
	logger.Debugf("Debug: %s %d", "test", 123)
	logger.Infof("Info: %s %d", "test", 456)
	logger.Warnf("Warn: %s %d", "test", 789)
	logger.Errorf("Error: %s %d", "test", 999)

	allLogs := logs.All()
	require.Len(t, allLogs, 4, "Should have captured 4 formatted log messages")

	// Verify message formatting
	expectedMessages := []string{
		"Debug: test 123",
		"Info: test 456",
		"Warn: test 789",
		"Error: test 999",
	}

	for i, log := range allLogs {
		assert.Equal(t, expectedMessages[i], log.Message, "Message %d should be formatted correctly", i)
	}
}

func TestLoggerWithFields(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	zapLogger := zap.New(core)
	logger := &Logger{
		z: zapLogger,
		l: zapr.NewLogger(zapLogger),
	}

	// Test WithField and WithFields
	logger.WithField("test", "1").Info("testing 1")
	logger.WithFields(map[string]interface{}{
		"test1": "1",
		"test2": 2,
	}).Info("testing 2")

	// Check results
	allLogs := logs.All()
	require.Len(t, allLogs, 2)

	// Helper function to get field value based on type
	getFieldValue := func(field zapcore.Field) interface{} {
		switch field.Type {
		case zapcore.StringType:
			return field.String
		case zapcore.Int64Type:
			return field.Integer
		default:
			return field.Interface
		}
	}

	// Verify first log
	firstLog := allLogs[0]
	require.Len(t, firstLog.Context, 1)
	assert.Equal(t, "test", firstLog.Context[0].Key)
	assert.Equal(t, "1", getFieldValue(firstLog.Context[0]))

	// Verify second log
	secondLog := allLogs[1]
	require.Len(t, secondLog.Context, 2)

	fieldMap := make(map[string]interface{})
	for _, field := range secondLog.Context {
		fieldMap[field.Key] = getFieldValue(field)
	}

	assert.Equal(t, "1", fieldMap["test1"])
	assert.Equal(t, int64(2), fieldMap["test2"])
}

func TestLogrInterface(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	zapLogger := zap.New(core)
	logger := &Logger{
		z: zapLogger,
		l: zapr.NewLogger(zapLogger),
	}

	logrLogger := logger.Logr()
	logrLogger.Info("This is from logr")
	logrLogger.V(1).Info("This is verbose") // Should be filtered out at info level

	allLogs := logs.All()
	require.Len(t, allLogs, 1, "Expected 1 log (verbose should be filtered)")
	assert.Equal(t, "This is from logr", allLogs[0].Message, "Should capture logr message correctly")
}
