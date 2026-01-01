package test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/HelloZhy/gotreedb/apiv1"
	"github.com/HelloZhy/gotreedb/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMain(m *testing.M) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	os.Exit(m.Run())
}

func newConn(t *testing.T) *grpc.ClientConn {
	conn, err := grpc.NewClient(
		"localhost:8080",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		t.Error(err)
	}

	return conn
}

func TestOneWriteClientTwoWatchClients(t *testing.T) {
	runner := server.New()
	runner.Start()
	defer runner.StopAndWait()

	writeClientTask := func(
		client apiv1.TreeDBClient,
		writeStartRxCh <-chan error,
		writeDoneTxCh chan<- error,
	) {
		defer close(writeDoneTxCh)

		if err := <-writeStartRxCh; err != nil {
			writeDoneTxCh <- err
			return
		}

		_, err := client.WriteBatch(
			context.Background(),
			&apiv1.WriteBatchReq{
				Ops: []*apiv1.WriteOp{
					{
						Entry: "/",
						Action: &apiv1.WriteOp_Create{
							Create: &apiv1.WriteActionCreate{
								Name: "node1",
							},
						},
					},
					{
						Entry: "/",
						Action: &apiv1.WriteOp_Create{
							Create: &apiv1.WriteActionCreate{
								Name: "node2",
							},
						},
					},
					{
						Entry: "/node1",
						Action: &apiv1.WriteOp_Update{
							Update: &apiv1.WriteActionUpdate{
								PutKeyToValue: map[string]string{
									"k1": "v1",
									"k2": "v2",
								},
							},
						},
					},
					{
						Entry: "/node2",
						Action: &apiv1.WriteOp_Update{
							Update: &apiv1.WriteActionUpdate{
								PutKeyToValue: map[string]string{
									"k3": "v3",
									"k4": "v4",
								},
							},
						},
					},
				},
			},
		)

		writeDoneTxCh <- err
	}

	watchClient1Task := func(
		client apiv1.TreeDBClient,
		watchTxCh chan<- *apiv1.WatchNotifyChResp,
	) {
		defer close(watchTxCh)

		stream, err := client.WatchNotifyCh(
			context.Background(),
			&apiv1.WatchNotifyChReq{
				Notifier: "client1",
				Entries:  []string{"/", "/node1"},
			},
		)

		if err != nil {
			t.Error(err)
			return
		}

		for {
			resp, err := stream.Recv()

			if err != nil {
				if !errors.Is(err, io.EOF) {
					t.Error(err)
				}
				return
			}

			t.Logf("client1 recv resp: %s", resp.String())

			watchTxCh <- resp
		}
	}

	watchClient2Task := func(
		client apiv1.TreeDBClient,
		watchTxCh chan<- *apiv1.WatchNotifyChResp,
	) {
		defer close(watchTxCh)

		stream, err := client.WatchNotifyCh(
			context.Background(),
			&apiv1.WatchNotifyChReq{
				Notifier: "client2",
				Entries:  []string{"/", "/node2"},
			},
		)

		if err != nil {
			t.Error(err)
			return
		}

		for {
			resp, err := stream.Recv()

			if err != nil {
				if !errors.Is(err, io.EOF) {
					t.Error(err)
				}
				return
			}

			t.Logf("client2 recv resp: %s", resp.String())

			watchTxCh <- resp
		}
	}

	readAllFromWatchClientsTask := func(
		watchRxCh1 chan *apiv1.WatchNotifyChResp,
		watchRxCh2 chan *apiv1.WatchNotifyChResp,
		doneTxCh chan error,
	) {
		defer close(doneTxCh)

		consumer := func(wg *sync.WaitGroup, rxCh chan *apiv1.WatchNotifyChResp) {
			defer wg.Done()
			for resp := range rxCh {
				t.Logf("consume resp: %s", resp.String())
			}
		}

		wg := sync.WaitGroup{}
		wg.Add(2)
		go consumer(&wg, watchRxCh1)
		go consumer(&wg, watchRxCh2)
		wg.Wait()

		doneTxCh <- nil
	}

	writeClientConn := newConn(t)
	defer writeClientConn.Close()
	writeClient := apiv1.NewTreeDBClient(writeClientConn)

	watchClientConn1 := newConn(t)
	defer watchClientConn1.Close()
	watchClient1 := apiv1.NewTreeDBClient(watchClientConn1)

	watchClientConn2 := newConn(t)
	defer watchClientConn2.Close()
	watchClient2 := apiv1.NewTreeDBClient(watchClientConn2)

	writeStartCh := make(chan error, 1)
	writeDoneCh := make(chan error, 1)
	go writeClientTask(writeClient, writeStartCh, writeDoneCh)

	watchCh1 := make(chan *apiv1.WatchNotifyChResp, 1)
	go watchClient1Task(watchClient1, watchCh1)
	watchCh2 := make(chan *apiv1.WatchNotifyChResp, 1)
	go watchClient2Task(watchClient2, watchCh2)

	doneCh := make(chan error, 1)
	go readAllFromWatchClientsTask(watchCh1, watchCh2, doneCh)

	select {
	case _, ok := <-watchCh1:
		if !ok {
			err := fmt.Errorf("unexpected watchCh1 closed")
			t.Error(err)
			writeStartCh <- err
		}
	case _, ok := <-watchCh2:
		if !ok {
			err := fmt.Errorf("unexpected watchCh2 closed")
			t.Error(err)
			writeStartCh <- err
		}
	default:
		writeStartCh <- nil
	}

	if err := <-writeDoneCh; err != nil {
		t.Error(err)
	}

	time.Sleep(time.Millisecond * 100)

	if _, err := watchClient1.CloseNotifyCh(
		context.Background(),
		&apiv1.CloseNotifyChReq{
			Notifier: "client1",
		},
	); err != nil {
		t.Error(err)
	}

	if _, err := watchClient2.CloseNotifyCh(
		context.Background(),
		&apiv1.CloseNotifyChReq{
			Notifier: "client2",
		},
	); err != nil {
		t.Error(err)
	}

	if err := <-doneCh; err != nil {
		t.Error(err)
	}
}
