package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"

	"github.com/HelloZhy/gotreedb/controller"
	"github.com/HelloZhy/gotreedb/resource"
)

func startup(
	_resource *resource.Resource,
	_controller *controller.Controller,
	mux *http.ServeMux,
) {
	_resource.RequestRecorder = resource.NewRequestRecorder()

	_controller.RequestsApi = &controller.RequestsApi{
		RequestRecorder: _resource.RequestRecorder,
	}

	_controller.RequestsApi.Register(mux)
}

func main() {
	_resource := resource.Resource{}
	_controller := controller.Controller{}

	mux := &http.ServeMux{}

	startup(&_resource, &_controller, mux)

	s := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	sigIntCh := make(chan os.Signal, 1)
	defer close(sigIntCh)

	signal.Notify(sigIntCh, os.Interrupt)
	defer signal.Ignore(os.Interrupt)

	closeCh := make(chan struct{}, 1)

	go func(closeTxCh chan<- struct{}) {
		defer close(closeTxCh)

		s.ListenAndServe()
	}(closeCh)

	<-sigIntCh

	s.Shutdown(context.Background())

	<-closeCh

	slog.Info(">>> finished")
}
