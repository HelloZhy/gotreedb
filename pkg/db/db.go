package db

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"
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

type ReadOp struct {
	Entry string
	Keys  []string
}

type ReadOpResult struct {
	Entry      string
	Exists     bool
	KeyToValue map[string]string
}

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
	log                       *slog.Logger
	nextId                    uint64
	idToNode                  map[uint64]*node
	notifierToCh              map[string]chan<- ReadOpResult
	entryToNotifiers          map[string]map[string]struct{}
	maxBufferSizeOfNotifierCh uint32
	txTimeoutOfNotifierCh     time.Duration
}

var errNotifierAlreadyExists = errors.New("notifier already exists")
var errNotifierFormatInvalid = errors.New("notifier format invalid")

func (db *TreeDB) validateNotifier(notifier string) bool {
	return validateStrOnlyContainWords(notifier)
}

func (db *TreeDB) RegisterNotifier(notifier string, entries []string) (ch <-chan ReadOpResult, err error) {
	db.log.Info("RegisterNotifier")
	db.log.Debug("RegisterNotifier args", slog.String("notifier", notifier), slog.Any("entries", entries))

	if len(entries) == 0 || !db.validateNotifier(notifier) {
		err = errNotifierFormatInvalid
		db.log.Error("RegisterNotifier failed", slog.String("err", err.Error()))
		return
	}

	for _, entry := range entries {
		if !db.validateEntry(entry) {
			err = errEntryFormatInvalid
			db.log.Error("RegisterNotifier failed", slog.String("err", err.Error()))
			return
		}
	}

	if _, ok := db.notifierToCh[notifier]; ok {
		err = errNotifierAlreadyExists
		db.log.Error("RegisterNotifier failed", slog.String("err", err.Error()))
		return
	}

	newCh := make(chan ReadOpResult, db.maxBufferSizeOfNotifierCh)

	db.notifierToCh[notifier] = newCh
	ch = newCh

	ops := make([]ReadOp, 0, len(entries))

	for _, entry := range entries {
		if _, ok := db.entryToNotifiers[entry]; !ok {
			db.entryToNotifiers[entry] = map[string]struct{}{}
		}

		notifiers := db.entryToNotifiers[entry]

		if _, ok := notifiers[notifier]; ok {
			panic(fmt.Sprintf("notifier should not exist, notifier: %s, db broken", notifier))
		}
		notifiers[notifier] = struct{}{}

		ops = append(ops, ReadOp{Entry: entry, Keys: nil})
	}

	db.readBatchAndNotify(ops)

	db.log.Info("RegisterNotifier done")

	return
}

func (db *TreeDB) UnregisterNotifier(notifier string) (err error) {
	db.log.Info("UnregisterNotifier")
	db.log.Debug("UnregisterNotifier args", slog.String("notifier", notifier))

	if !db.validateNotifier(notifier) {
		err = errNotifierFormatInvalid
		db.log.Error("UnregisterNotifier failed", slog.String("err", err.Error()))
		return
	}

	entriesToBeRemoved := []string{}
	for entry, notifiers := range db.entryToNotifiers {
		delete(notifiers, notifier)
		if len(notifiers) == 0 {
			entriesToBeRemoved = append(entriesToBeRemoved, entry)
		}
	}

	for _, entry := range entriesToBeRemoved {
		delete(db.entryToNotifiers, entry)
	}

	if ch, ok := db.notifierToCh[notifier]; ok {
		close(ch)
	}
	delete(db.notifierToCh, notifier)

	db.log.Info("UnregisterNotifier done")

	return
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

func (db *TreeDB) ReadBatch(ops ...ReadOp) (results []ReadOpResult, err error) {
	db.log.Info("ReadBatch")
	db.log.Debug("ReadBatch args", slog.Any("ops", ops))

	for _, op := range ops {
		if !db.validateEntry(op.Entry) {
			err = errEntryFormatInvalid
			slog.Error("ReadBatch failed", slog.String("err", err.Error()))
			return
		}
	}

	results = make([]ReadOpResult, 0, len(ops))

	for _, op := range ops {
		db.log.Debug("ReadOp", slog.Any("op", op))

		opResult := ReadOpResult{
			Entry:      op.Entry,
			Exists:     false,
			KeyToValue: map[string]string{},
		}

		if _node := db.findNode(op.Entry); _node != nil {
			opResult.Exists = true

			if len(op.Keys) > 0 {
				for _, key := range op.Keys {
					if value, ok := _node.keyToValue[key]; ok {
						opResult.KeyToValue[key] = value
					}
				}
			} else {
				opResult.KeyToValue = maps.Clone(_node.keyToValue)
			}
		}

		results = append(results, opResult)

		db.log.Debug("ReadOp done")
	}

	db.log.Info("ReadBatch done")

	return
}

type backupNodeWithNames struct {
	names []string
	node  *node
}

func (db *TreeDB) buildBackupNodesWithNames(ops []WriteOp) (backupNodesWithNames []backupNodeWithNames) {
	db.log.Debug("buildBackupNodesWithNames", slog.Any("ops", ops))

	backupEntryToNode := map[string]*node{}
	for _, op := range ops {
		entry := op.Entry
		_node := db.findNode(entry)
		if _node == nil {
			continue
		}

		// backup current node
		backupEntryToNode[entry] = _node.clone()

		// backup child nodes
		if op.Delete != nil {
			childEntry := db.concatEntryAndName(op.Entry, op.Delete.Name)
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

var errEntryAlreadyExists = errors.New("entry already exists in db")

var errIdAlreadyExists = errors.New("node id already exists")
var errIdNotExists = errors.New("node id not exists")

var errNotLeafNode = errors.New("not leaf node")

var errEntryFormatInvalid = errors.New("entry format invalid")

var errNameFormatInvalid = errors.New("name format invalid")

func validateStrOnlyContainWords(s string) bool {
	validateChar := func(ch rune) bool {
		return ('a' <= ch && ch <= 'z') ||
			('A' <= ch && ch <= 'Z') ||
			('0' <= ch && ch <= '9') ||
			ch == '_' ||
			ch == '-'
	}

	for _, ch := range s {
		if !validateChar(ch) {
			return false
		}
	}

	return true
}

func (db *TreeDB) Close() {
	db.log.Info("Close")
	defer db.log.Info("Close done")

	db.entryToNotifiers = nil
	for _, ch := range db.notifierToCh {
		close(ch)
	}
	db.notifierToCh = nil
}

func (db *TreeDB) validateName(name string) bool {
	return validateStrOnlyContainWords(name)
}

func (db *TreeDB) concatEntryAndName(entry, name string) string {
	if !db.validateEntry(entry) || !db.validateName(name) {
		panic(fmt.Sprintf("entry: %s or name: %s not valid", entry, name))
	}

	if entry == "/" {
		return entry + name
	} else {
		return fmt.Sprintf("%s/%s", entry, name)
	}
}

func (db *TreeDB) readBatchAndNotify(ops []ReadOp) {
	results, err := db.ReadBatch(ops...)
	if err != nil {
		db.log.Error("do ReadBatch failed after WriteOps done", slog.String("err", err.Error()))
		return
	}

	for _, result := range results {
		for notifier := range db.entryToNotifiers[result.Entry] {
			ch := db.notifierToCh[notifier]
			select {
			case ch <- result:
			case <-time.After(db.txTimeoutOfNotifierCh):
				db.log.Error(
					"channel write failed, maybe full?",
					slog.String("notifier", notifier),
					slog.Any("txTimeoutOfNotifierCh", db.txTimeoutOfNotifierCh),
				)
			}
		}
	}
}

func (db *TreeDB) buildReadOpsReadBatchAndNotify(writeOps []WriteOp) {
	entries := map[string]struct{}{}
	readOps := []ReadOp{}

	for _, op := range writeOps {
		var entry string
		if op.Create != nil {
			entry = db.concatEntryAndName(op.Entry, op.Create.Name)
		} else if op.Delete != nil {
			entry = db.concatEntryAndName(op.Entry, op.Delete.Name)
		} else if op.Update != nil {
			entry = op.Entry
		}

		if _, ok := entries[entry]; !ok {
			readOps = append(readOps, ReadOp{Entry: entry, Keys: nil})
			entries[entry] = struct{}{}
		}
	}

	db.readBatchAndNotify(readOps)
}

func (db *TreeDB) WriteBatch(ops ...WriteOp) (err error) {
	db.log.Info("WriteBatch")
	db.log.Debug("WriteBatch args", slog.Any("ops", ops))

	for _, op := range ops {
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
		db.log.Error("WriteBatch failed", slog.String("err", err.Error()))
		return
	}

	backupNodesWithNames := db.buildBackupNodesWithNames(ops)
	newAddedNodeIds := []uint64{}

	for _, op := range ops {
		db.log.Debug("WriteOp", slog.Any("op.Entry", op.Entry))

		_node := db.findNode(op.Entry)

		var _newNode *node

		var delNodeId uint64
		var delNode *node

		if _node == nil {
			err = errEntryNotExists
			db.log.Error("WriteBatch failed", slog.String("err", err.Error()), slog.String("entry", op.Entry))
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
				db.log.Error("WriteBatch failed", slog.String("err", err.Error()))
				goto REVERT_POINT
			}

			delNodeId = _node.nameToIds[op.Delete.Name]
			if _, ok := db.idToNode[delNodeId]; !ok {
				err = errIdNotExists
				db.log.Error("WriteBatch failed", slog.String("err", err.Error()))
				goto REVERT_POINT
			}

			delNode = db.idToNode[delNodeId]

			if len(delNode.nameToIds) > 0 {
				err = errNotLeafNode
				db.log.Error("WriteBatch failed", slog.String("err", err.Error()))
				goto REVERT_POINT
			}

			delete(db.idToNode, _node.nameToIds[op.Delete.Name])
			delete(_node.nameToIds, op.Delete.Name)
		} else if op.Update != nil {
			db.log.Debug("", slog.Any("op.Update", *(op.Update)))

			maps.Copy(_node.keyToValue, op.Update.PutKeyToValue)

			for _, k := range op.Update.DeleteKeys {
				delete(_node.keyToValue, k)
			}
		}

		db.log.Debug("WriteOp done")
	}

	db.buildReadOpsReadBatchAndNotify(ops)

	db.log.Info("WriteBatch done")

	return nil

REVERT_POINT:
	db.log.Info("WriteBatch revert")
	db.log.Debug(
		"WriteBatch revert args",
		slog.Any("backupNodesWithNames", backupNodesWithNames),
		slog.Any("newAddedNodeIds", newAddedNodeIds),
	)
	for _, backupNodeWithNames := range backupNodesWithNames {
		db.log.Debug("", slog.Any("names", backupNodeWithNames.names), slog.Any("_node", *(backupNodeWithNames.node)))
	}
	for _, nodeWithNames := range backupNodesWithNames {
		db.idToNode[nodeWithNames.node.id] = nodeWithNames.node
	}

	for _, id := range newAddedNodeIds {
		delete(db.idToNode, id)
	}
	for id, _node := range db.idToNode {
		db.log.Debug("", slog.Uint64("id", id), slog.Any("_node", *_node))
	}

	db.log.Debug("WriteBatch revert done")

	return err
}

const defaultMaxBufferSizeOfNotifierCh uint32 = 16
const defaultTxTimeoutOfNotifierCh = time.Millisecond * 50

func New() *TreeDB {
	db := &TreeDB{
		log: slog.Default().With(
			slog.String("mod", "gotreedb"),
			slog.String("pkg", "db"),
			slog.String("ext-info", ""),
		),
		idToNode:                  map[uint64]*node{},
		notifierToCh:              map[string]chan<- ReadOpResult{},
		entryToNotifiers:          map[string]map[string]struct{}{},
		maxBufferSizeOfNotifierCh: defaultMaxBufferSizeOfNotifierCh,
		txTimeoutOfNotifierCh:     defaultTxTimeoutOfNotifierCh,
	}

	rootNode := db.newNode("")
	db.idToNode[rootNode.id] = rootNode

	return db
}
