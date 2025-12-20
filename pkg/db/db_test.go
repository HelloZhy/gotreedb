package db

import (
	"log/slog"
	"os"
	"testing"
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

	batch := WriteBatch{
		Ops: []WriteOp{
			{
				Entry: "/",
				Update: &WriteActionUpdate{
					PutKeyToValue: map[string]string{
						"k1": "v1",
					},
				},
			},
			{
				Entry: "/",
				Create: &WriteActionCreate{
					Name: "node1",
				},
			},
			{
				Entry: "/node1",
				Update: &WriteActionUpdate{
					PutKeyToValue: map[string]string{
						"k1": "v1",
						"k2": "v2",
					},
				},
			},
			{
				Entry: "/",
				Create: &WriteActionCreate{
					Name: "node2",
				},
			},
			{
				Entry: "/node2",
				Update: &WriteActionUpdate{
					PutKeyToValue: map[string]string{
						"k1": "v1",
						"k2": "v2",
						"k3": "v3",
					},
				},
			},
			{
				Entry: "/",
				Create: &WriteActionCreate{
					Name: "node3",
				},
			},
			{
				Entry: "/node1",
				Create: &WriteActionCreate{
					Name: "node11",
				},
			},
			{
				Entry: "/node2",
				Create: &WriteActionCreate{
					Name: "node21",
				},
			},
			{
				Entry: "/node2",
				Create: &WriteActionCreate{
					Name: "node22",
				},
			},
			{
				Entry: "/node2/node22",
				Update: &WriteActionUpdate{
					PutKeyToValue: map[string]string{
						"k1": "v1",
					},
				},
			},
			{
				Entry: "/node1/node11",
				Create: &WriteActionCreate{
					Name: "node111",
				},
			},
			{
				Entry: "/node2/node21",
				Create: &WriteActionCreate{
					Name: "node211",
				},
			},
			{
				Entry: "/node2/node21/node211",
				Update: &WriteActionUpdate{
					PutKeyToValue: map[string]string{
						"k1": "v1",
						"k2": "v2",
					},
				},
			},
		},
	}

	if err := db.WriteBatch(batch); err != nil {
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

	batch := ReadBatch{
		Ops: []ReadOp{
			// read specific key in entry
			{
				Entry: "/",
				Keys:  []string{"k1"},
			},
			{
				Entry: "/node1",
				Keys:  []string{"k1", "k2"},
			},
			{
				Entry: "/node2",
				Keys:  []string{"k1", "k2", "k3"},
			},
			{
				Entry: "/node2/node22",
				Keys:  []string{"k1"},
			},
			{
				Entry: "/node2/node21/node211",
				Keys:  []string{"k1", "k2"},
			},
			// read all keys in entry
			{
				Entry: "/",
				Keys:  nil,
			},
			{
				Entry: "/node1",
				Keys:  nil,
			},
			{
				Entry: "/node2",
				Keys:  nil,
			},
			{
				Entry: "/node2/node22",
				Keys:  nil,
			},
			{
				Entry: "/node2/node21/node211",
				Keys:  nil,
			},
		},
	}

	result, err := db.ReadBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", result)
}

func TestReadBatchFailed(t *testing.T) {
	t.Parallel()

	db, err := initTestDB()
	if err != nil {
		t.Fatal(err)
	}

	{
		batch := ReadBatch{
			Ops: []ReadOp{
				// read key in entry
				{
					Entry: "/",
					Keys:  []string{"k1"},
				},
				// read unknown key in entry
				{
					Entry: "/",
					Keys:  []string{"kx"},
				},
			},
		}

		if _, err := db.ReadBatch(batch); err == nil {
			t.Fatal("should return error, but not")
		}
	}

	{
		batch := ReadBatch{
			Ops: []ReadOp{
				// read key in entry
				{
					Entry: "/",
					Keys:  []string{"k1"},
				},
				// invalid entry
				{
					Entry: "/node2/",
					Keys:  []string{"k1"},
				},
			},
		}

		if _, err := db.ReadBatch(batch); err == nil {
			t.Fatal("should return error, but not")
		}
	}

	{
		batch := ReadBatch{
			Ops: []ReadOp{
				// read key in entry
				{
					Entry: "/",
					Keys:  []string{"k1"},
				},
				// read entry not exists
				{
					Entry: "/nodeUnkown",
					Keys:  []string{"k1"},
				},
			},
		}

		if _, err := db.ReadBatch(batch); err == nil {
			t.Fatal("should return error, but not")
		}
	}
}

func TestWriteBatchSuccess(t *testing.T) {
	t.Parallel()

	db, err := initTestDB()
	if err != nil {
		t.Fatal(err)
	}

	writeBatch := WriteBatch{
		Ops: []WriteOp{
			// update to node already exists in db
			{
				Entry: "/",
				Update: &WriteActionUpdate{
					PutKeyToValue: map[string]string{
						"k2": "v2",
					},
					DeleteKeys: []string{"k1"},
				},
			},
			{
				Entry: "/node2",
				Update: &WriteActionUpdate{
					PutKeyToValue: map[string]string{
						"k2": "v4",
					},
					DeleteKeys: []string{"k1", "k3"},
				},
			},
			// create new node to a parent node that already exists in db
			{
				Entry: "/",
				Create: &WriteActionCreate{
					Name: "node4",
				},
			},
			{
				Entry: "/node1",
				Create: &WriteActionCreate{
					Name: "node12",
				},
			},
			// delete a node that already exists in db
			{
				Entry: "/",
				Delete: &WriteActionDelete{
					Name: "node3",
				},
			},
			// update on new created node
			{
				Entry: "/node4",
				Update: &WriteActionUpdate{
					PutKeyToValue: map[string]string{
						"k1": "v1",
						"k2": "v2",
					},
				},
			},
			// create new node to a new created parent node
			{
				Entry: "/node4",
				Create: &WriteActionCreate{
					Name: "node41",
				},
			},
			// delete a node that is new created
			{
				Entry: "/node1",
				Delete: &WriteActionDelete{
					Name: "node12",
				},
			},
		},
	}

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

	if err := db.WriteBatch(writeBatch); err != nil {
		t.Fatal(err)
	}

	readBatch := ReadBatch{
		Ops: []ReadOp{
			{
				Entry: "/",
				Keys:  []string{"k2"},
			},
			{
				Entry: "/node2",
				Keys:  []string{"k2"},
			},
			{
				Entry: "/node4",
				Keys:  []string{"k1", "k2"},
			},
			{
				Entry: "/node4/node41",
				Keys:  nil,
			},
		},
	}

	result, err := db.ReadBatch(readBatch)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%+v", result)
}

func TestWriteBatchFailed(t *testing.T) {
	t.Parallel()

	db, err := initTestDB()
	if err != nil {
		t.Fatal(err)
	}

	writeBatch := WriteBatch{
		Ops: []WriteOp{
			// update to node already exists in db
			{
				Entry: "/",
				Update: &WriteActionUpdate{
					PutKeyToValue: map[string]string{
						"k2": "v2",
					},
					DeleteKeys: []string{"k1"},
				},
			},
			{
				Entry: "/node2",
				Update: &WriteActionUpdate{
					PutKeyToValue: map[string]string{
						"k2": "v4",
					},
					DeleteKeys: []string{"k1", "k3"},
				},
			},
			// create new node to a parent node that already exists in db
			{
				Entry: "/",
				Create: &WriteActionCreate{
					Name: "node4",
				},
			},
			{
				Entry: "/node1",
				Create: &WriteActionCreate{
					Name: "node12",
				},
			},
			// delete a node that already exists in db
			{
				Entry: "/",
				Delete: &WriteActionDelete{
					Name: "node3",
				},
			},
			// update on new created node
			{
				Entry: "/node4",
				Update: &WriteActionUpdate{
					PutKeyToValue: map[string]string{
						"k1": "v1",
						"k2": "v2",
					},
				},
			},
			// create new node to a new created parent node
			{
				Entry: "/node4",
				Create: &WriteActionCreate{
					Name: "node41",
				},
			},
			// delete a node that is new created
			{
				Entry: "/node1",
				Delete: &WriteActionDelete{
					Name: "node12",
				},
			},
			// delete node4 should be rejected and return error because node4 has child nodes
			{
				Entry: "/",
				Delete: &WriteActionDelete{
					Name: "node4",
				},
			},
		},
	}

	if err = db.WriteBatch(writeBatch); err == nil {
		t.Fatal("should return error, but not")
	}

	// validate WriteBatch should be reverted if error happened
	batch := ReadBatch{
		Ops: []ReadOp{
			// read specific key in entry
			{
				Entry: "/",
				Keys:  []string{"k1"},
			},
			{
				Entry: "/node1",
				Keys:  []string{"k1", "k2"},
			},
			{
				Entry: "/node2",
				Keys:  []string{"k1", "k2", "k3"},
			},
			{
				Entry: "/node2/node22",
				Keys:  []string{"k1"},
			},
			{
				Entry: "/node2/node21/node211",
				Keys:  []string{"k1", "k2"},
			},
			// read all keys in entry
			{
				Entry: "/",
				Keys:  nil,
			},
			{
				Entry: "/node1",
				Keys:  nil,
			},
			{
				Entry: "/node2",
				Keys:  nil,
			},
			{
				Entry: "/node2/node22",
				Keys:  nil,
			},
			{
				Entry: "/node2/node21/node211",
				Keys:  nil,
			},
		},
	}

	result, err := db.ReadBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", result)

	writeBatch = WriteBatch{
		Ops: []WriteOp{
			{
				Entry: "/",
				Create: &WriteActionCreate{
					Name: "abc-012/daf",
				},
			},
		},
	}

	if err = db.WriteBatch(writeBatch); err == nil {
		t.Fatal("should return error, but not")
	}
}
