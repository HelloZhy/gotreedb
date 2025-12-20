package db

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	slog.SetLogLoggerLevel(slog.LevelDebug)

	os.Exit(m.Run())
}

func initTestDB() (*TreeDB, error) {
	db := NewTreeDB()

	// / k1: v1
	// /node1 k1: v1, k2: v2
	// /node2 k1: v1, k2: v2, k3: v3
	// /node3
	// /node1/node11
	// /node2/node21
	// /node2/node22 k1: v1
	// /node1/node11/node111
	// /node2/node21/node211 k1: v1, k2: v2

	if err := db.WriteBatch(
		WriteOp{
			Entry: "/",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k1": "v1",
				},
			},
		},
		WriteOp{
			Entry: "/",
			Create: &WriteActionCreate{
				Name: "node1",
			},
		},
		WriteOp{
			Entry: "/node1",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k1": "v1",
					"k2": "v2",
				},
			},
		},
		WriteOp{
			Entry: "/",
			Create: &WriteActionCreate{
				Name: "node2",
			},
		},
		WriteOp{
			Entry: "/node2",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k1": "v1",
					"k2": "v2",
					"k3": "v3",
				},
			},
		},
		WriteOp{
			Entry: "/",
			Create: &WriteActionCreate{
				Name: "node3",
			},
		},
		WriteOp{
			Entry: "/node1",
			Create: &WriteActionCreate{
				Name: "node11",
			},
		},
		WriteOp{
			Entry: "/node2",
			Create: &WriteActionCreate{
				Name: "node21",
			},
		},
		WriteOp{
			Entry: "/node2",
			Create: &WriteActionCreate{
				Name: "node22",
			},
		},
		WriteOp{
			Entry: "/node2/node22",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k1": "v1",
				},
			},
		},
		WriteOp{
			Entry: "/node1/node11",
			Create: &WriteActionCreate{
				Name: "node111",
			},
		},
		WriteOp{
			Entry: "/node2/node21",
			Create: &WriteActionCreate{
				Name: "node211",
			},
		},
		WriteOp{
			Entry: "/node2/node21/node211",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k1": "v1",
					"k2": "v2",
				},
			},
		},
	); err != nil {
		return nil, err
	}

	return db, nil
}

func TestReadBatchSuccess(t *testing.T) {
	t.Parallel()

	db, err := initTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	results, err := db.ReadBatch(
		// read specific key in entry
		ReadOp{
			Entry: "/",
			Keys:  []string{"k1"},
		},
		ReadOp{
			Entry: "/node1",
			Keys:  []string{"k1", "k2"},
		},
		ReadOp{
			Entry: "/node2",
			Keys:  []string{"k1", "k2", "k3"},
		},
		ReadOp{
			Entry: "/node2/node22",
			Keys:  []string{"k1"},
		},
		ReadOp{
			Entry: "/node2/node21/node211",
			Keys:  []string{"k1", "k2"},
		},
		// read all keys in entry
		ReadOp{
			Entry: "/",
			Keys:  nil,
		},
		ReadOp{
			Entry: "/node1",
			Keys:  nil,
		},
		ReadOp{
			Entry: "/node2",
			Keys:  nil,
		},
		ReadOp{
			Entry: "/node2/node22",
			Keys:  nil,
		},
		ReadOp{
			Entry: "/node2/node21/node211",
			Keys:  nil,
		},
		// read entry not exists in db
		ReadOp{
			Entry: "/abc",
			Keys:  nil,
		},
		// read key not exists in entry
		ReadOp{
			Entry: "/node1",
			Keys:  []string{"k3"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", results)
}

func TestReadBatchFailed(t *testing.T) {
	t.Parallel()

	db, err := initTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.ReadBatch(
		// read key in entry
		ReadOp{
			Entry: "/",
			Keys:  []string{"k1"},
		},
		// invalid entry
		ReadOp{
			Entry: "/node2/",
			Keys:  []string{"k1"},
		},
	); err == nil {
		t.Fatal("should return error, but not")
	}
}

func TestWriteBatchSuccess(t *testing.T) {
	t.Parallel()

	db, err := initTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// / k2: v2
	// /node1 k1: v1, k2: v2
	// /node2 k2: v4
	// /node4 k1: v1, k2: v2
	// /node1/node11
	// /node2/node21
	// /node2/node22 k1: v1
	// /node4/node41
	// /node1/node11/node111
	// /node2/node21/node211 k1: v1, k2: v2

	if err := db.WriteBatch(
		// update to node already exists in db
		WriteOp{
			Entry: "/",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k2": "v2",
				},
				DeleteKeys: []string{"k1"},
			},
		},
		WriteOp{
			Entry: "/node2",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k2": "v4",
				},
				DeleteKeys: []string{"k1", "k3"},
			},
		},
		// create new node to a parent node that already exists in db
		WriteOp{
			Entry: "/",
			Create: &WriteActionCreate{
				Name: "node4",
			},
		},
		WriteOp{
			Entry: "/node1",
			Create: &WriteActionCreate{
				Name: "node12",
			},
		},
		// delete a node that already exists in db
		WriteOp{
			Entry: "/",
			Delete: &WriteActionDelete{
				Name: "node3",
			},
		},
		// update on new created node
		WriteOp{
			Entry: "/node4",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k1": "v1",
					"k2": "v2",
				},
			},
		},
		// create new node to a new created parent node
		WriteOp{
			Entry: "/node4",
			Create: &WriteActionCreate{
				Name: "node41",
			},
		},
		// delete a node that is new created
		WriteOp{
			Entry: "/node1",
			Delete: &WriteActionDelete{
				Name: "node12",
			},
		},
	); err != nil {
		t.Fatal(err)
	}

	results, err := db.ReadBatch(
		ReadOp{
			Entry: "/",
			Keys:  []string{"k2"},
		},
		ReadOp{
			Entry: "/node2",
			Keys:  []string{"k2"},
		},
		ReadOp{
			Entry: "/node4",
			Keys:  []string{"k1", "k2"},
		},
		ReadOp{
			Entry: "/node4/node41",
			Keys:  nil,
		},
		// read entry not exists in db
		ReadOp{
			Entry: "/node1/node12",
			Keys:  nil,
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%+v", results)
}

func TestWriteBatchFailed(t *testing.T) {
	t.Parallel()

	db, err := initTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err = db.WriteBatch(
		// update to node already exists in db
		WriteOp{
			Entry: "/",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k2": "v2",
				},
				DeleteKeys: []string{"k1"},
			},
		},
		WriteOp{
			Entry: "/node2",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k2": "v4",
				},
				DeleteKeys: []string{"k1", "k3"},
			},
		},
		// create new node to a parent node that already exists in db
		WriteOp{
			Entry: "/",
			Create: &WriteActionCreate{
				Name: "node4",
			},
		},
		WriteOp{
			Entry: "/node1",
			Create: &WriteActionCreate{
				Name: "node12",
			},
		},
		// delete a node that already exists in db
		WriteOp{
			Entry: "/",
			Delete: &WriteActionDelete{
				Name: "node3",
			},
		},
		// update on new created node
		WriteOp{
			Entry: "/node4",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k1": "v1",
					"k2": "v2",
				},
			},
		},
		// create new node to a new created parent node
		WriteOp{
			Entry: "/node4",
			Create: &WriteActionCreate{
				Name: "node41",
			},
		},
		// delete a node that is new created
		WriteOp{
			Entry: "/node1",
			Delete: &WriteActionDelete{
				Name: "node12",
			},
		},
		// delete node4 should be rejected and return error because node4 has child nodes
		WriteOp{
			Entry: "/",
			Delete: &WriteActionDelete{
				Name: "node4",
			},
		},
	); err == nil {
		t.Fatal("should return error, but not")
	}

	// validate WriteBatch should be reverted if error happened
	results, err := db.ReadBatch(
		// read specific key in entry
		ReadOp{
			Entry: "/",
			Keys:  []string{"k1"},
		},
		ReadOp{
			Entry: "/node1",
			Keys:  []string{"k1", "k2"},
		},
		ReadOp{
			Entry: "/node2",
			Keys:  []string{"k1", "k2", "k3"},
		},
		ReadOp{
			Entry: "/node2/node22",
			Keys:  []string{"k1"},
		},
		ReadOp{
			Entry: "/node2/node21/node211",
			Keys:  []string{"k1", "k2"},
		},
		// read all keys in entry
		ReadOp{
			Entry: "/",
			Keys:  nil,
		},
		ReadOp{
			Entry: "/node1",
			Keys:  nil,
		},
		ReadOp{
			Entry: "/node2",
			Keys:  nil,
		},
		ReadOp{
			Entry: "/node2/node22",
			Keys:  nil,
		},
		ReadOp{
			Entry: "/node2/node21/node211",
			Keys:  nil,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", results)

	if err = db.WriteBatch(
		WriteOp{
			Entry: "/",
			Create: &WriteActionCreate{
				Name: "abc-012/daf",
			},
		},
	); err == nil {
		t.Fatal("should return error, but not")
	}
}

func TestRegisterUnregisterNotifierAndWriteNotifySuccess(t *testing.T) {
	db, err := initTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rxCh, err := db.RegisterNotifier(
		"test-notifier",
		[]string{
			"/",
			"/node1",
			"/node4",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.UnregisterNotifier("test-notifier")

	doneCh := make(chan error, 1)
	startRxCh := make(chan struct{}, 1)
	go func() {
		defer close(doneCh)

		<-startRxCh

		// receive init data after RegisterNotifier
		select {
		case msg := <-rxCh:
			if msg.Entry == "/" {
				t.Logf("%+v", msg)
			}
		case <-time.After(time.Millisecond * 100):
			doneCh <- errors.New("timeout")
			return
		}

		select {
		case msg := <-rxCh:
			if msg.Entry == "/node1" {
				t.Logf("%+v", msg)
			}
		case <-time.After(time.Millisecond * 100):
			doneCh <- errors.New("timeout")
			return
		}

		select {
		case msg := <-rxCh:
			if msg.Entry == "/node4" {
				t.Logf("%+v", msg)
			}
		case <-time.After(time.Millisecond * 100):
			doneCh <- errors.New("timeout")
			return
		}

		// receive update after WriteBatch
		select {
		case msg := <-rxCh:
			if msg.Entry == "/" {
				t.Logf("%+v", msg)
			}
		case <-time.After(time.Millisecond * 100):
			doneCh <- errors.New("timeout")
			return
		}

		select {
		case msg := <-rxCh:
			if msg.Entry == "/node1" {
				t.Logf("%+v", msg)
			}
		case <-time.After(time.Millisecond * 100):
			doneCh <- errors.New("timeout")
			return
		}

		select {
		case msg := <-rxCh:
			if msg.Entry == "/node4" {
				t.Logf("%+v", msg)
			}
		case <-time.After(time.Millisecond * 100):
			doneCh <- errors.New("timeout")
			return
		}

		select {
		case msg := <-rxCh:
			doneCh <- fmt.Errorf("should not receive this msg: %+v", msg)
			return
		case <-time.After(time.Millisecond * 100):
		}

		doneCh <- nil
	}()

	if err := db.WriteBatch(
		WriteOp{
			Entry: "/",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k3": "v3",
					"k4": "v4",
				},
				DeleteKeys: []string{
					"k1",
				},
			},
		},
		WriteOp{
			Entry: "/node1",
			Update: &WriteActionUpdate{
				PutKeyToValue: map[string]string{
					"k1": "v4",
				},
			},
		},
		WriteOp{
			Entry: "/",
			Create: &WriteActionCreate{
				Name: "node4",
			},
		},
		WriteOp{
			Entry: "/",
			Delete: &WriteActionDelete{
				Name: "node4",
			},
		},
	); err != nil {
		t.Fatal(err)
	}

	close(startRxCh)

	if err := <-doneCh; err != nil {
		t.Fatal(err)
	}
}

func TestRegisterNotifierFailed(t *testing.T) {
	t.Parallel()

	db, err := initTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.RegisterNotifier("validNotifier", []string{}); err == nil {
		t.Fatal("should return error, but not")
	}

	if _, err = db.RegisterNotifier("invalid@notifier", []string{"/"}); err == nil {
		t.Fatal("should return error, but not")
	}

	if _, err = db.RegisterNotifier("validNotifier", []string{"invalid"}); err == nil {
		t.Fatal("should return error, but not")
	}

	if _, err = db.RegisterNotifier("validNotifier", []string{"/node/"}); err == nil {
		t.Fatal("should return error, but not")
	}

	if _, err = db.RegisterNotifier("duplicateNotifier", []string{"/"}); err != nil {
		t.Fatal(err)
	}
	if _, err = db.RegisterNotifier("duplicateNotifier", []string{"/node"}); err == nil {
		t.Fatal("should return error, but not")
	}
}

func TestUnregisterNotifierFailed(t *testing.T) {
	t.Parallel()

	db, err := initTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.UnregisterNotifier("invalid@notifier"); err == nil {
		t.Fatal("should return error, but not")
	}
}
