package resource

import (
	"errors"
	"log/slog"
	"slices"
	"sync"

	"github.com/google/uuid"
)

type Request struct {
	ReadBatch  *ReadBatch
	WriteBatch *WriteBatch
}

func (r Request) clone() (req Request) {
	if r.ReadBatch != nil {
		req.ReadBatch = new(ReadBatch)
		*req.ReadBatch = r.ReadBatch.clone()
	} else if r.WriteBatch != nil {
		req.WriteBatch = new(WriteBatch)
		*req.WriteBatch = r.WriteBatch.clone()
	}

	return
}

type RequestRecord struct {
	UUID    uuid.UUID
	Request Request
}

func (r RequestRecord) clone() (record RequestRecord) {
	record.UUID = r.UUID
	record.Request = r.Request.clone()

	return
}

type RequestRecorder struct {
	mu      sync.RWMutex
	records []RequestRecord
}

func (r *RequestRecorder) Append(req Request) uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()

	_uuid := uuid.New()
	record := RequestRecord{
		UUID:    _uuid,
		Request: req.clone(),
	}

	r.records = append(r.records, record)

	return _uuid
}

func (r *RequestRecorder) FetchAll() []RequestRecord {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return slices.Clone(r.records)
}

var errRecordNotFound = errors.New("record not found")

func IsErrRecordNotFound(err error) bool {
	return errors.Is(err, errRecordNotFound)
}

func (r *RequestRecorder) Fetch(_uuid uuid.UUID) (record RequestRecord, err error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, _record := range r.records {
		if _record.UUID == _uuid {
			slog.Debug("record found", slog.Any("record", _record))
			return _record.clone(), nil
		}
	}
	slog.Error("record not found", slog.Any("uuid", _uuid))

	return RequestRecord{}, errRecordNotFound
}

func NewRequestRecorder() *RequestRecorder {
	return &RequestRecorder{
		mu:      sync.RWMutex{},
		records: []RequestRecord{},
	}
}
