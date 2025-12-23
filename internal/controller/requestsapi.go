package controller

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"slices"

	"github.com/HelloZhy/gotreedb/internal/resource"
	"github.com/google/uuid"
)

type RequestsApi struct {
	RequestRecorder *resource.RequestRecorder
}

func (api *RequestsApi) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/requests", api.getAll)
	mux.HandleFunc("GET /api/requests/{uuid}", api.getOne)
	mux.HandleFunc("POST /api/requests", api.post)
}

type Request struct {
	ReadBatch  *ReadBatch  `json:"read_batch"`
	WriteBatch *WriteBatch `json:"write_batch"`
}

func (r *Request) load(req *resource.Request) {
	if req.ReadBatch != nil {
		r.ReadBatch = new(ReadBatch)
		r.ReadBatch.load(req.ReadBatch)
	} else if req.WriteBatch != nil {
		r.WriteBatch = new(WriteBatch)
		r.WriteBatch.load(req.WriteBatch)
	}
}

func (r *Request) build() (req resource.Request) {
	if r.ReadBatch != nil {
		req.ReadBatch = new(resource.ReadBatch)
		*req.ReadBatch = r.ReadBatch.build()
	} else if r.WriteBatch != nil {
		req.WriteBatch = new(resource.WriteBatch)
		*req.WriteBatch = r.WriteBatch.build()
	}

	return
}

type RequestRecord struct {
	UUID    uuid.UUID `json:"uuid"`
	Request Request   `json:"request"`
}

func (r *RequestRecord) load(record *resource.RequestRecord) {
	r.UUID = record.UUID
	r.Request.load(&record.Request)
}

type ReadBatch struct {
	Ops []ReadOp `json:"ops"`
}

func (b *ReadBatch) load(batch *resource.ReadBatch) {
	b.Ops = make([]ReadOp, len(batch.Ops))
	for i, op := range batch.Ops {
		b.Ops[i].load(op)
	}
}

func (b *ReadBatch) build() (batch resource.ReadBatch) {
	batch.Ops = make([]resource.ReadOp, len(b.Ops))
	for i, op := range b.Ops {
		batch.Ops[i] = op.build()
	}

	return
}

type ReadOp struct {
	Entry string   `json:"entry"`
	Keys  []string `json:"keys"`
}

func (o *ReadOp) load(op resource.ReadOp) {
	o.Entry = op.Entry
	o.Keys = op.Keys
}

func (o *ReadOp) build() (op resource.ReadOp) {
	op.Entry = o.Entry
	op.Keys = o.Keys

	return
}

type WriteBatch struct {
	Ops []WriteOp `json:"ops"`
}

func (b *WriteBatch) load(batch *resource.WriteBatch) {
	b.Ops = make([]WriteOp, len(batch.Ops))
	for i, op := range batch.Ops {
		b.Ops[i].load(op)
	}
}

func (b *WriteBatch) build() (batch resource.WriteBatch) {
	batch.Ops = make([]resource.WriteOp, len(b.Ops))
	for i, op := range b.Ops {
		batch.Ops[i] = op.build()
	}

	return
}

type KVPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (_kv *KVPair) load(kv resource.KVPair) {
	_kv.Key = kv.Key
	_kv.Value = kv.Value
}

func (_kv *KVPair) build() (kv resource.KVPair) {
	kv.Key = _kv.Key
	kv.Value = _kv.Value

	return
}

type CreateAction struct {
	KVPairs  []KVPair `json:"kv_pairs"`
	Override bool     `json:"override"`
}

func (a *CreateAction) load(act *resource.CreateAction) {
	a.KVPairs = make([]KVPair, len(act.KVPairs))
	for i, kv := range act.KVPairs {
		a.KVPairs[i].load(kv)
	}
	a.Override = act.Override
}

func (a *CreateAction) build() (act resource.CreateAction) {
	act.KVPairs = make([]resource.KVPair, len(a.KVPairs))
	for i, kv := range a.KVPairs {
		act.KVPairs[i] = kv.build()
	}
	act.Override = a.Override
	return
}

type UpdateAction struct {
	KVPairs []KVPair `json:"kv_pairs"`
}

func (a *UpdateAction) load(act *resource.UpdateAction) {
	a.KVPairs = make([]KVPair, len(act.KVPairs))
	for i, kv := range act.KVPairs {
		a.KVPairs[i].load(kv)
	}
}

func (a *UpdateAction) build() (act resource.UpdateAction) {
	act.KVPairs = make([]resource.KVPair, len(a.KVPairs))
	for i, kv := range a.KVPairs {
		act.KVPairs[i] = kv.build()
	}
	return
}

type DeleteAction struct {
	Keys []string `json:"keys"`
}

func (a *DeleteAction) load(act *resource.DeleteAction) {
	a.Keys = slices.Clone(act.Keys)
}

func (a *DeleteAction) build() (act resource.DeleteAction) {
	act.Keys = slices.Clone(a.Keys)
	return
}

type WriteOp struct {
	Entry        string        `json:"entry"`
	CreateAction *CreateAction `json:"create_action"`
	UpdateAction *UpdateAction `json:"update_action"`
	DeleteAction *DeleteAction `json:"delete_action"`
}

func (o *WriteOp) load(op resource.WriteOp) {
	o.Entry = op.Entry
	if op.CreateAction != nil {
		o.CreateAction.load(op.CreateAction)
	} else if op.UpdateAction != nil {
		o.UpdateAction.load(op.UpdateAction)
	} else if op.DeleteAction != nil {
		o.DeleteAction.load(op.DeleteAction)
	}
}

func (o *WriteOp) build() (op resource.WriteOp) {
	op.Entry = o.Entry
	if o.CreateAction != nil {
		op.CreateAction = new(resource.CreateAction)
		*op.CreateAction = o.CreateAction.build()
	} else if o.UpdateAction != nil {
		op.UpdateAction = new(resource.UpdateAction)
		*op.UpdateAction = o.UpdateAction.build()
	} else if o.DeleteAction != nil {
		op.DeleteAction = new(resource.DeleteAction)
		*op.DeleteAction = o.DeleteAction.build()
	}

	return
}

type GetRequestsResp struct {
	RequestRecord []RequestRecord `json:"request_records"`
}

func (api *RequestsApi) getAll(w http.ResponseWriter, r *http.Request) {
	_records := api.RequestRecorder.FetchAll()
	records := make([]RequestRecord, len(_records))
	for i, _record := range _records {
		records[i].load(&_record)
	}
	writeJsonResp(w, GetRequestsResp{RequestRecord: records})
}

type GetRequestResp struct {
	RequestRecord RequestRecord `json:"request_record"`
}

func (api *RequestsApi) getOne(w http.ResponseWriter, r *http.Request) {
	uuidStr := r.PathValue("uuid")
	slog.Debug("", slog.String("uuid", uuidStr))
	var _uuid uuid.UUID
	if err := _uuid.UnmarshalText([]byte(uuidStr)); err != nil {
		writeErrJsonResp(w, err)
		return
	}

	_record, err := api.RequestRecorder.Fetch(_uuid)
	if err != nil {
		writeErrJsonResp(w, err)
		return
	}

	record := RequestRecord{}
	record.load(&_record)
	writeJsonResp(w, GetRequestResp{RequestRecord: record})
}

type PostRequestReq = Request
type PostRequestResp struct {
	UUID uuid.UUID `json:"uuid"`
}

func (api *RequestsApi) post(w http.ResponseWriter, r *http.Request) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		writeErrJsonResp(w, err)
		return
	}

	req := PostRequestReq{}
	if err := json.Unmarshal(b, &req); err != nil {
		writeErrJsonResp(w, err)
		return
	}

	_uuid := api.RequestRecorder.Append(req.build())

	writeJsonResp(w, PostRequestResp{UUID: _uuid})
}
