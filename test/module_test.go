package test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/HelloZhy/gotreedb/internal/server"
	"github.com/HelloZhy/gotreedb/pkg/apiv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestMain(m *testing.M) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	os.Exit(m.Run())
}

func TestSmoke(t *testing.T) {
	runner := server.New()
	runner.Start()
	defer runner.StopAndWait()

	clients := []apiv1.TreeDBClient{}
	for range 2 {
		conn, err := grpc.NewClient(
			"localhost:8080",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Error(err)
		}
		defer conn.Close()

		clients = append(clients, apiv1.NewTreeDBClient(conn))
	}

	// client0
	client0DoneCh := make(chan error, 1)
	go func() {
		defer close(client0DoneCh)

		respStream, err := clients[0].WatchNotifyCh(
			context.Background(),
			&apiv1.WatchNotifyChReq{
				Notifier: "client0-notifier",
				Entries:  []string{"/", "/node1", "/node2"},
			},
		)

		if err != nil {
			client0DoneCh <- err
			return
		}

		cnt := 0

		for {
			msg, err := respStream.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					client0DoneCh <- err
					return
				} else {
					break
				}
			}

			t.Logf("client0 recv resp: %s", msg.String())

			cnt++
		}

		client0DoneCh <- nil
	}()

	// client1 write
	if _, err := clients[1].WriteBatch(
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
	); err != nil {
		t.Error(err)
	}

	resp, err := clients[1].ReadBatch(
		context.Background(),
		&apiv1.ReadBatchReq{
			Ops: []*apiv1.ReadOp{
				{
					Entry: "/node1",
					Keys:  nil,
				},
				{
					Entry: "/node2",
					Keys:  nil,
				},
			},
		},
	)
	if err != nil {
		t.Error(err)
	}
	t.Log(resp)

	time.Sleep(time.Millisecond * 100)

	if _, err := clients[0].CloseNotifyCh(
		context.Background(),
		&apiv1.CloseNotifyChReq{
			Notifier: "client0-notifier",
		},
	); err != nil {
		t.Error(err)
	}

	if err := <-client0DoneCh; err != nil {
		t.Error(err)
	}
}
