package db

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
)

type node struct {
	id         uint64
	name       string
	keyToValue map[string]string
	nameToIds  map[string]uint64
}

func (n *node) clone() *node {
	return &node{
		id:         n.id,
		name:       n.name,
		keyToValue: maps.Clone(n.keyToValue),
		nameToIds:  maps.Clone(n.nameToIds),
	}
}

type idToNode map[uint64]*node

type ReadOp struct {
	Entry string
	Keys  []string
}

type ReadOpResult struct {
	Entry      string
	KeyToValue map[string]string
}

type ReadBatch struct {
	Ops []ReadOp
}

type ReadBatchResult struct {
	OpResults []ReadOpResult
}

type WriteBatch struct {
	Ops []WriteOp
}

type WriteOpType string

type WriteOp struct {
	Entry  string
	Create *WriteActionCreate
	Delete *WriteActionDelete
	Update *WriteActionUpdate
}

type WriteActionCreate struct {
	Name string
}

type WriteActionDelete struct {
	Name string
}

type WriteActionUpdate struct {
	PutKeyToValue map[string]string
	DeleteKeys    []string
}

type TreeDB struct {
	log      *slog.Logger
	nextId   uint64
	idToNode idToNode
}

func (db *TreeDB) rootNode() *node {
	rootNode, ok := db.idToNode[0]
	if !ok {
		panic("db.idToNode[0] (root) not found, db broken")
	}
	return rootNode
}

func (db *TreeDB) validateEntry(entry string) bool {
	if !strings.HasPrefix(entry, "/") {
		return false
	}

	if len(entry) > 1 {
		if strings.HasSuffix(entry, "/") {
			return false
		}
	}

	return true
}

func (db *TreeDB) convertEntryToNodeNames(entry string) (nodeNames []string) {
	if len(entry) == 0 {
		return
	}

	if entry[0] == '/' {
		entry = entry[1:]
	}

	if len(entry) == 0 {
		return
	}

	nodeNames = strings.Split(entry, "/")

	return
}

func (db *TreeDB) findNode(entry string) *node {
	nodeNames := db.convertEntryToNodeNames(entry)

	var curNode *node = db.rootNode()
	for _, nodeName := range nodeNames {

		childNodeId, ok := curNode.nameToIds[nodeName]
		if !ok {
			return nil
		}
		childNode, ok := db.idToNode[childNodeId]
		if !ok {
			panic(fmt.Sprintf("nodeName: %s exists, but childNodeId: %d not found in db.idToNode, db broken", nodeName, childNodeId))
		}
		curNode = childNode
	}

	return curNode
}

func (db *TreeDB) allocId() uint64 {
	newId := db.nextId
	db.nextId++
	return newId
}

func (db *TreeDB) newNode(name string) *node {
	return &node{
		id:         db.allocId(),
		name:       name,
		keyToValue: make(map[string]string),
		nameToIds:  make(map[string]uint64),
	}
}

var errEntryNotExists = errors.New("entry not exists in db")
var errKeyNotExists = errors.New("key not exists in entry")

func (db *TreeDB) ReadBatch(batch ReadBatch) (result ReadBatchResult, err error) {
	db.log.Info("ReadBatch")
	defer db.log.Info("ReadBatch done")

	for _, op := range batch.Ops {
		if !db.validateEntry(op.Entry) {
			return result, errEntryFormatInvalid
		}
	}

	result.OpResults = make([]ReadOpResult, 0, len(batch.Ops))

	for _, op := range batch.Ops {
		db.log.Debug("ReadOp", slog.Any("op", op))

		_node := db.findNode(op.Entry)
		if _node == nil {
			err = errEntryNotExists
			db.log.Error("error occured", slog.String("err", err.Error()), slog.String("entry", op.Entry))
			return
		}

		opResult := ReadOpResult{
			Entry:      op.Entry,
			KeyToValue: map[string]string{},
		}

		if len(op.Keys) > 0 {
			for _, key := range op.Keys {
				value, ok := _node.keyToValue[key]
				if !ok {
					err = errKeyNotExists
					db.log.Error("error occured", slog.String("err", err.Error()), slog.String("key", key))
					return
				}
				opResult.KeyToValue[key] = value
			}
		} else {
			opResult.KeyToValue = maps.Clone(_node.keyToValue)
		}

		result.OpResults = append(result.OpResults, opResult)

		db.log.Debug("ReadOp done")
	}

	return result, nil
}

type backupNodeWithNames struct {
	names []string
	node  *node
}

func (db *TreeDB) buildBackupNodesWithNames(batch WriteBatch) (backupNodesWithNames []backupNodeWithNames) {
	db.log.Debug("buildBackupNodesWithNames", slog.Any("batch", batch))

	backupEntryToNode := map[string]*node{}
	for _, op := range batch.Ops {
		entry := op.Entry
		_node := db.findNode(entry)
		if _node == nil {
			continue
		}

		// backup current node
		backupEntryToNode[entry] = _node.clone()

		// backup child nodes
		if op.Delete != nil {
			childEntry := fmt.Sprintf("%s/%s", op.Entry, op.Delete.Name)
			childNodeId, ok := _node.nameToIds[op.Delete.Name]
			if !ok {
				continue
			}
			childNode, ok := db.idToNode[childNodeId]
			if !ok {
				continue
			}
			backupEntryToNode[childEntry] = childNode.clone()
		}
	}
	db.log.Debug("", slog.Any("backupEntryToNode", backupEntryToNode))

	backupNodesWithNames = make([]backupNodeWithNames, 0, len(backupEntryToNode))
	for entry, _node := range backupEntryToNode {
		backupNodesWithNames = append(backupNodesWithNames, backupNodeWithNames{
			names: db.convertEntryToNodeNames(entry),
			node:  _node,
		})
	}
	slices.SortFunc(backupNodesWithNames, func(a, b backupNodeWithNames) int {
		return slices.Compare(a.names, b.names)
	})

	db.log.Debug("buildBackupNodesWithNames done", slog.Any("backupNodesWithNames", backupNodesWithNames))
	return
}

var errWriteActionNotSupported = errors.New("write action not supported")

var errEntryAlreadyExists = errors.New("entry already exists in db")

var errIdAlreadyExists = errors.New("node id already exists")
var errIdNotExists = errors.New("node id not exists")

var errNotLeafNode = errors.New("not leaf node")

var errEntryFormatInvalid = errors.New("entry format invalid")

var errNameFormatInvalid = errors.New("name format invalid")

func (db *TreeDB) validateName(name string) bool {
	validateChar := func(ch rune) bool {
		return ('a' <= ch && ch <= 'z') ||
			('A' <= ch && ch <= 'Z') ||
			('0' <= ch && ch <= '9') ||
			ch == '_' ||
			ch == '-'
	}

	for _, ch := range name {
		if !validateChar(ch) {
			return false
		}
	}

	return true
}

func (db *TreeDB) WriteBatch(batch WriteBatch) (err error) {
	db.log.Info("WriteBatch")
	defer db.log.Info("WriteBatch done")

	for _, op := range batch.Ops {
		if !db.validateEntry(op.Entry) {
			err = errEntryFormatInvalid
			break
		}

		if op.Create != nil {
			if !db.validateName(op.Create.Name) {
				err = errNameFormatInvalid
				break
			}
		} else if op.Delete != nil {
			if !db.validateName(op.Delete.Name) {
				err = errNameFormatInvalid
				break
			}
		}
	}

	if err != nil {
		slog.Error("", slog.String("err", err.Error()))
		return
	}

	backupNodesWithNames := db.buildBackupNodesWithNames(batch)
	newAddedNodeIds := []uint64{}

	for _, op := range batch.Ops {
		db.log.Debug("WriteOp", slog.Any("op.Entry", op.Entry))

		_node := db.findNode(op.Entry)

		var _newNode *node

		var delNodeId uint64
		var delNode *node

		if _node == nil {
			err = errEntryNotExists
			db.log.Error("error occured", slog.String("err", err.Error()), slog.String("entry", op.Entry))
			goto REVERT_POINT
		}
		db.log.Debug("findNode done", slog.Any("_node", *_node))

		if op.Create != nil {
			db.log.Debug("WriteActionCreate", slog.Any("op.Create", *(op.Create)))

			_newNode = db.newNode(op.Create.Name)
			db.log.Debug("newNode done", slog.Any("_newNode", *_newNode))

			if _, ok := _node.nameToIds[_newNode.name]; ok {
				err = errEntryAlreadyExists
				db.log.Error("error occured", slog.String("err", err.Error()))
				goto REVERT_POINT
			}

			if _, ok := db.idToNode[_newNode.id]; ok {
				err = errIdAlreadyExists
				db.log.Error("error occured", slog.String("err", err.Error()))
				goto REVERT_POINT
			}

			_node.nameToIds[_newNode.name] = _newNode.id
			db.idToNode[_newNode.id] = _newNode

			newAddedNodeIds = append(newAddedNodeIds, _newNode.id)

			db.log.Debug("WriteActionCreate done")
		} else if op.Delete != nil {
			db.log.Debug("", slog.Any("op.Delete", *(op.Delete)))

			if _, ok := _node.nameToIds[op.Delete.Name]; !ok {
				err = errEntryNotExists
				db.log.Error("error occured", slog.String("err", err.Error()))
				goto REVERT_POINT
			}

			delNodeId = _node.nameToIds[op.Delete.Name]
			if _, ok := db.idToNode[delNodeId]; !ok {
				err = errIdNotExists
				db.log.Error("error occured", slog.String("err", err.Error()))
				goto REVERT_POINT
			}

			delNode = db.idToNode[delNodeId]

			if len(delNode.nameToIds) > 0 {
				err = errNotLeafNode
				db.log.Error("error occured", slog.String("err", err.Error()))
				goto REVERT_POINT
			}

			delete(db.idToNode, _node.nameToIds[op.Delete.Name])
		} else if op.Update != nil {
			db.log.Debug("", slog.Any("op.Update", *(op.Update)))

			maps.Copy(_node.keyToValue, op.Update.PutKeyToValue)

			for _, k := range op.Update.DeleteKeys {
				delete(_node.keyToValue, k)
			}
		} else {
			err = errWriteActionNotSupported
			db.log.Error("error occured", slog.String("err", err.Error()))
			goto REVERT_POINT
		}

		db.log.Debug("WriteOp done")
	}

	return nil

REVERT_POINT:
	db.log.Debug(
		"WriteBatch revert",
		slog.Any("backupNodesWithNames", backupNodesWithNames),
		slog.Any("newAddedNodeIds", newAddedNodeIds),
	)
	for _, backupNodeWithNames := range backupNodesWithNames {
		slog.Debug("", slog.Any("names", backupNodeWithNames.names), slog.Any("_node", *(backupNodeWithNames.node)))
	}
	for _, nodeWithNames := range backupNodesWithNames {
		db.idToNode[nodeWithNames.node.id] = nodeWithNames.node
	}

	for _, id := range newAddedNodeIds {
		delete(db.idToNode, id)
	}
	for id, _node := range db.idToNode {
		slog.Debug("", slog.Uint64("id", id), slog.Any("_node", *_node))
	}
	db.log.Debug("WriteBatch revert done")

	return err
}

func NewTreeDB() *TreeDB {
	db := &TreeDB{
		log:      slog.Default().With(slog.String("mod", "gotreedb")),
		idToNode: idToNode{},
	}

	rootNode := db.newNode("")
	db.idToNode[rootNode.id] = rootNode

	return db
}
