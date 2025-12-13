package resource

import "slices"

type KVPair struct {
	Key   string
	Value string
}

type ReadBatch struct {
	Ops []ReadOp
}

func (b ReadBatch) clone() (batch ReadBatch) {
	for _, op := range b.Ops {
		batch.Ops = append(batch.Ops, op.clone())
	}

	return
}

type ReadOp struct {
	Entry string
	Keys  []string
}

func (o ReadOp) clone() (op ReadOp) {
	op.Entry = o.Entry
	op.Keys = slices.Clone(o.Keys)

	return
}

type WriteBatch struct {
	Ops []WriteOp
}

func (b WriteBatch) clone() (batch WriteBatch) {
	for _, op := range b.Ops {
		b.Ops = append(b.Ops, op.clone())
	}

	return
}

type CreateAction struct {
	KVPairs  []KVPair
	Override bool
}

func (a CreateAction) clone() (act CreateAction) {
	act.KVPairs = slices.Clone(a.KVPairs)
	act.Override = a.Override

	return
}

type UpdateAction struct {
	KVPairs []KVPair
}

func (a UpdateAction) clone() (act UpdateAction) {
	act.KVPairs = slices.Clone(a.KVPairs)

	return
}

type DeleteAction struct {
	Keys []string
}

func (a DeleteAction) clone() (act DeleteAction) {
	act.Keys = slices.Clone(a.Keys)

	return
}

type WriteOp struct {
	Entry        string
	CreateAction *CreateAction
	UpdateAction *UpdateAction
	DeleteAction *DeleteAction
}

func (o WriteOp) clone() (op WriteOp) {
	op.Entry = o.Entry

	if o.CreateAction != nil {
		op.CreateAction = new(CreateAction)
		*op.CreateAction = o.CreateAction.clone()
	} else if o.UpdateAction != nil {
		op.UpdateAction = new(UpdateAction)
		*op.UpdateAction = o.UpdateAction.clone()
	} else if o.DeleteAction != nil {
		op.DeleteAction = new(DeleteAction)
		*op.DeleteAction = o.DeleteAction.clone()
	}

	return
}
