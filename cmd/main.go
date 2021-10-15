package main

import (
	"github.com/jpillora/opts"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	initLogger()
	opts.Parse(globalOption).RunFatal()
}

func initLogger() {
	config := zap.NewProductionConfig()
	config.Encoding = "console"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, _ := config.Build()
	defer logger.Sync()
	zap.ReplaceGlobals(logger)
}
