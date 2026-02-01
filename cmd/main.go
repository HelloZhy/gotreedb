package main

import (
	"log/slog"
	"os"
	"os/signal"

	"github.com/HelloZhy/gotreedb/internal/server"
)

func main() {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	r := server.New()

	sigIntCh := make(chan os.Signal, 1)
	defer close(sigIntCh)

	signal.Notify(sigIntCh, os.Interrupt)
	defer signal.Ignore(os.Interrupt)

	r.Start()

	<-sigIntCh

	r.StopAndWait()
}
