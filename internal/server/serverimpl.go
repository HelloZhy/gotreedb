package server

import (
	"context"
	"log/slog"
	"maps"
	"slices"

	"github.com/HelloZhy/gotreedb/pkg/apiv1"
	"github.com/HelloZhy/gotreedb/pkg/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type serverImpl struct {
	apiv1.UnimplementedTreeDBServer
	log  *slog.Logger
	txCh chan<- any
}

func (impl *serverImpl) ReadBatch(ctx context.Context, req *apiv1.ReadBatchReq) (*apiv1.ReadBatchResp, error) {
	impl.log.Info("ReadBatch")
	impl.log.Debug("ReadBatch args", slog.String("req", req.String()))

	ops := make([]db.ReadOp, 0, len(req.Ops))

	for _, op := range req.Ops {
		ops = append(
			ops,
			db.ReadOp{
				Entry: op.Entry,
				Keys:  slices.Clone(op.Keys),
			},
		)
	}

	msgCh := make(chan readBatchResp, 1)

	impl.txCh <- &readBatchReq{
		Ops:  ops,
		TxCh: msgCh,
	}

	internalResp := <-msgCh

	if internalResp.Err != nil {
		impl.log.Error("ReadBatch failed", slog.String("err", internalResp.Err.Error()))
		return nil, status.Error(codes.NotFound, internalResp.Err.Error())
	}

	results := make([]*apiv1.ReadOpResult, 0, len(internalResp.Results))
	for _, result := range internalResp.Results {
		results = append(results, &apiv1.ReadOpResult{
			Entry:      result.Entry,
			Exists:     result.Exists,
			KeyToValue: maps.Clone(result.KeyToValue),
		})
	}

	resp := &apiv1.ReadBatchResp{
		Results: results,
	}

	impl.log.Info("ReadBatch done")

	return resp, nil
}

func (impl *serverImpl) WriteBatch(ctx context.Context, req *apiv1.WriteBatchReq) (*apiv1.WriteBatchResp, error) {
	impl.log.Info("WriteBatch")
	impl.log.Debug("WriteBatch args", slog.String("req", req.String()))

	ops := make([]db.WriteOp, 0, len(req.Ops))

	for _, op := range req.Ops {
		var _op db.WriteOp

		switch action := op.Action.(type) {
		case *apiv1.WriteOp_Create:
			_op.Entry = op.Entry
			_op.Create = &db.WriteActionCreate{Name: action.Create.Name}
		case *apiv1.WriteOp_Delete:
			_op.Entry = op.Entry
			_op.Delete = &db.WriteActionDelete{Name: action.Delete.Name}
		case *apiv1.WriteOp_Update:
			_op.Entry = op.Entry
			_op.Update = &db.WriteActionUpdate{
				PutKeyToValue: maps.Clone(action.Update.PutKeyToValue),
				DeleteKeys:    slices.Clone(action.Update.DeleteKeys),
			}
		}

		ops = append(ops, _op)
	}

	msgCh := make(chan writeBatchResp, 1)

	impl.txCh <- &writeBatchReq{
		Ops:  ops,
		TxCh: msgCh,
	}

	internalResp := <-msgCh

	if internalResp.Err != nil {
		impl.log.Error("WriteBatch failed", slog.String("err", internalResp.Err.Error()))
		return nil, status.Error(codes.NotFound, internalResp.Err.Error())
	}

	impl.log.Info("WriteBatch done")

	return &apiv1.WriteBatchResp{}, nil
}

func (impl *serverImpl) notifyLoop(
	doneTxCh chan<- error,
	rxCh <-chan db.ReadOpResult,
	stream grpc.ServerStreamingServer[apiv1.WatchNotifyChResp],
) {
	defer close(doneTxCh)

	for result := range rxCh {
		err := stream.Send(&apiv1.WatchNotifyChResp{
			Result: &apiv1.ReadOpResult{
				Entry:      result.Entry,
				Exists:     result.Exists,
				KeyToValue: maps.Clone(result.KeyToValue),
			},
		})

		if err != nil {
			impl.log.Error("notifyLoop failed", slog.String("err", err.Error()))
			doneTxCh <- err
			return
		}
	}

	doneTxCh <- nil
}

func (impl *serverImpl) WatchNotifyCh(req *apiv1.WatchNotifyChReq, stream grpc.ServerStreamingServer[apiv1.WatchNotifyChResp]) error {
	impl.log.Info("WatchNotifyCh")
	impl.log.Debug("WatchNotifyCh args", slog.String("req", req.String()))

	msgCh := make(chan registerResp, 1)

	impl.txCh <- &registerReq{
		Notifier: req.Notifier,
		Entries:  slices.Clone(req.Entries),
		TxCh:     msgCh,
	}

	internalResp := <-msgCh

	if internalResp.Err != nil {
		impl.log.Error("WatchNotifyCh failed", slog.String("err", internalResp.Err.Error()))
		return status.Error(codes.NotFound, internalResp.Err.Error())
	}

	doneCh := make(chan error, 1)
	go impl.notifyLoop(doneCh, internalResp.RxCh, stream)

	if err := <-doneCh; err != nil {
		impl.log.Info("WatchNotifyCh loopback CloseNotifyChReq")
		if _, err := impl.CloseNotifyCh(
			context.Background(),
			&apiv1.CloseNotifyChReq{Notifier: req.Notifier},
		); err != nil {
			impl.log.Error("WatchNotifyCh loopback CloseNotifyChReq failed", slog.String("err", err.Error()))
			return status.Error(codes.NotFound, internalResp.Err.Error())
		}
		impl.log.Info("WatchNotifyCh loopback CloseNotifyChReq done")
	}

	impl.log.Info("WatchNotifyCh done")

	return nil
}

func (impl *serverImpl) CloseNotifyCh(ctx context.Context, req *apiv1.CloseNotifyChReq) (*apiv1.CloseNotifyChResp, error) {
	impl.log.Info("CloseNotifyCh")
	impl.log.Debug("CloseNotifyCh args", slog.String("req", req.String()))

	msgCh := make(chan unregisterResp, 1)

	impl.txCh <- &unregisterReq{
		Notifier: req.Notifier,
		TxCh:     msgCh,
	}

	internalResp := <-msgCh

	if internalResp.Err != nil {
		impl.log.Error("CloseNotifyCh failed", slog.String("err", internalResp.Err.Error()))
		return nil, status.Error(codes.NotFound, internalResp.Err.Error())
	}

	impl.log.Info("CloseNotifyCh done")

	return &apiv1.CloseNotifyChResp{}, nil
}

func (impl *serverImpl) Close() {
	close(impl.txCh)
}
