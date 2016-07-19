package odb

import (
	"github.com/yangchenxing/go-orderedset"
)

// Interface represent the object
type Object interface {
	PrimaryKey() int64
	IntField(string) int64
	StringField(string) string
}

type objectItem struct {
	o Object
}

func (item objectItem) Less(than orderedset.Item) bool {
	other := than.(objectItem)
	return item.o.PrimaryKey() < other.o.PrimaryKey()
}

func wrapObjectItem(o Object) objectItem {
	return objectItem{
		o: o,
	}
}

type keyObject int64

func (o keyObject) Less(than orderedset.Item) bool {
	return int64(o) < than.(Object).PrimaryKey()
}
