package server

import (
	"log/slog"
	"net"
	"os"

	"github.com/HelloZhy/gotreedb/apiv1"
	"github.com/HelloZhy/gotreedb/db"
	"google.golang.org/grpc"
)

type Runner struct {
	log        *slog.Logger
	handler    *handler
	server     *serverImpl
	grpcServer *grpc.Server
	closeCh    chan struct{}
}

func (r *Runner) Start() error {
	r.log.Info("Start")

	if err := r.handler.Start(); err != nil {
		r.log.Error("Start failed", slog.String("err", err.Error()))
		return err
	}

	apiv1.RegisterTreeDBServer(r.grpcServer, r.server)

	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		r.log.Error("Start failed", slog.String("err", err.Error()))
		return err
	}

	go r.loop(lis)

	r.log.Info("Start done")

	return nil
}

func (r *Runner) loop(lis net.Listener) {
	defer close(r.closeCh)

	if err := r.grpcServer.Serve(lis); err != nil {
		r.log.Error("grpcServer.Serve failed", slog.String("err", err.Error()))
		proc, _ := os.FindProcess(os.Getpid())
		proc.Signal(os.Interrupt)
	}
}

func (r *Runner) StopAndWait() {
	r.log.Info("StopAndWait")

	r.grpcServer.GracefulStop()

	<-r.closeCh

	r.server.Close()
	r.handler.StopAndWait()

	r.log.Info("StopAndWait done")
}

func New() *Runner {
	log := slog.Default().With(
		slog.String("mod", "gotreedb"),
		slog.String("pkg", "server"),
	)
	ch := make(chan any, 1024)

	return &Runner{
		log: log.With(slog.String("ext-info", "runner")),
		handler: &handler{
			log:     log.With(slog.String("ext-info", "handler")),
			treeDB:  db.New(),
			rxCh:    ch,
			closeCh: make(chan struct{}),
			doneCh:  make(chan struct{}),
		},
		server: &serverImpl{
			log:  slog.Default().With(slog.String("ext-info", "server")),
			txCh: ch,
		},
		grpcServer: grpc.NewServer(),
		closeCh:    make(chan struct{}, 1),
	}
}
