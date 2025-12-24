package server

import (
	"log/slog"

	"github.com/HelloZhy/gotreedb/pkg/db"
)

type handler struct {
	log     *slog.Logger
	treeDB  *db.TreeDB
	rxCh    <-chan any
	closeCh chan struct{}
	doneCh  chan struct{}
}

type readBatchResp struct {
	Results []db.ReadOpResult
	Err     error
}

type readBatchReq struct {
	Ops  []db.ReadOp
	TxCh chan<- readBatchResp
}

type writeBatchResp struct {
	Err error
}

type writeBatchReq struct {
	Ops  []db.WriteOp
	TxCh chan<- writeBatchResp
}

type registerResp struct {
	RxCh <-chan db.ReadOpResult
	Err  error
}

type registerReq struct {
	Notifier string
	Entries  []string
	TxCh     chan<- registerResp
}

type unregisterResp struct {
	Err error
}

type unregisterReq struct {
	Notifier string
	TxCh     chan<- unregisterResp
}

func (h *handler) Start() error {
	go h.loop()

	return nil
}

func (h *handler) handleReadBatchReq(msg *readBatchReq) {
	h.log.Debug("handleReadBatchReq")
	h.log.Debug("handleReadBatchReq args", slog.Any("msg", *msg))

	results, err := h.treeDB.ReadBatch(msg.Ops...)

	resp := readBatchResp{
		Results: results,
		Err:     err,
	}
	h.log.Debug("handleReadBatchReq send", slog.Any("resp", resp))

	msg.TxCh <- resp
	close(msg.TxCh)

	h.log.Debug("handleReadBatchReq done")
}

func (h *handler) handleWriteBatchReq(msg *writeBatchReq) {
	h.log.Debug("handleWriteBatchReq")
	h.log.Debug("handleWriteBatchReq args", slog.Any("msg", *msg))

	err := h.treeDB.WriteBatch(msg.Ops...)

	resp := writeBatchResp{Err: err}
	h.log.Debug("handleWriteBatchReq send", slog.Any("resp", resp))

	msg.TxCh <- resp
	close(msg.TxCh)

	h.log.Debug("handleWriteBatchReq done")
}

func (h *handler) handleRegisterReq(msg *registerReq) {
	h.log.Debug("handleRegisterReq")
	h.log.Debug("handleRegisterReq args", slog.Any("msg", *msg))

	ch, err := h.treeDB.RegisterNotifier(msg.Notifier, msg.Entries)

	resp := registerResp{
		RxCh: ch,
		Err:  err,
	}
	h.log.Debug("handleRegisterReq send", slog.Any("resp", resp))

	msg.TxCh <- resp
	close(msg.TxCh)

	h.log.Debug("handleRegisterReq done")
}

func (h *handler) handleUnregisterReq(msg *unregisterReq) {
	h.log.Debug("handleUnregisterReq")
	h.log.Debug("handleUnregisterReq args", slog.Any("msg", *msg))

	err := h.treeDB.UnregisterNotifier(msg.Notifier)

	resp := unregisterResp{Err: err}
	h.log.Debug("handleUnregisterReq send", slog.Any("resp", resp))

	msg.TxCh <- resp
	close(msg.TxCh)

	h.log.Debug("handleUnregisterReq done")
}

func (h *handler) handleAnyMsg(m any) {
	switch msg := m.(type) {
	case *readBatchReq:
		h.handleReadBatchReq(msg)
	case *writeBatchReq:
		h.handleWriteBatchReq(msg)
	case *registerReq:
		h.handleRegisterReq(msg)
	case *unregisterReq:
		h.handleUnregisterReq(msg)
	}
}

func (h *handler) loop() {
	defer close(h.doneCh)

	h.log.Info("loop")
	defer h.log.Info("loop done")

	for {
		select {
		case <-h.closeCh:
			return
		case m := <-h.rxCh:
			h.handleAnyMsg(m)
		}
	}
}

func (h *handler) StopAndWait() {
	h.log.Info("StopAndWait")

	close(h.closeCh)
	<-h.doneCh

	h.treeDB.Close()

	h.log.Info("StopAndWait done")
}
