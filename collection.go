package odb

import (
	"sync"

	"github.com/yangchenxing/go-orderedset"
)

type indexType int

const (
	intIndex indexType = iota
	stringIndex
)

type Collection struct {
	sync.RWMutex
	items         orderedset.OrderedSet
	intIndexes    map[string]map[int64]orderedset.OrderedSet
	stringIndexes map[string]map[string]orderedset.OrderedSet
	IndexSetMaker func() orderedset.OrderedSet
	QuerySetMaker func() orderedset.OrderedSet
}

func (collection *Collection) AddIntIndex(name string) {
	collection.Lock()
	defer collection.Unlock()
	if collection.intIndexes == nil {
		collection.intIndexes = make(map[string]map[int64]orderedset.OrderedSet)
	}
	collection.intIndexes[name] = make(map[int64]orderedset.OrderedSet)
}

func (collection *Collection) AddStringIndex(name string) {
	collection.Lock()
	defer collection.Unlock()
	if collection.stringIndexes == nil {
		collection.stringIndexes = make(map[string]map[string]orderedset.OrderedSet)
	}
	collection.stringIndexes[name] = make(map[string]orderedset.OrderedSet)
}

func (collection *Collection) ReplaceOrInsert(o Object) {
	collection.Lock()
	defer collection.Unlock()
	if collection.items == nil {
		collection.items = collection.IndexSetMaker()
	}
	item := wrapObjectItem(o)
	collection.items.ReplaceOrInsert(item)
	for name := range collection.intIndexes {
		value := o.IntField(name)
		set := collection.intIndexes[name][value]
		if set == nil {
			set = collection.IndexSetMaker()
			collection.intIndexes[name][value] = set
		}
		set.ReplaceOrInsert(item)
	}
	for name := range collection.stringIndexes {
		value := o.StringField(name)
		set := collection.stringIndexes[name][value]
		if set == nil {
			set = collection.IndexSetMaker()
			collection.stringIndexes[name][value] = set
		}
		set.ReplaceOrInsert(item)
	}
}

func (collection *Collection) Delete(o Object) {
	collection.Lock()
	defer collection.Unlock()
	if collection.items == nil {
		return
	}
	item := wrapObjectItem(o)
	for name := range collection.intIndexes {
		value := o.IntField(name)
		set := collection.intIndexes[name][value]
		if set != nil {
			set.Delete(item)
		}
	}
	for name := range collection.stringIndexes {
		value := o.StringField(name)
		set := collection.stringIndexes[name][value]
		if set != nil {
			set.Delete(item)
		}
	}
	collection.items.Delete(item)
}

func (collection *Collection) Get(key int64) Object {
	collection.RLock()
	defer collection.RUnlock()
	if collection.items == nil {
		return nil
	}
	if item := collection.items.Get(keyObject(key)); item != nil {
		return item.(Object)
	}
	return nil
}

func (collection *Collection) Query(query Query) orderedset.OrderedSet {
	collection.RLock()
	defer collection.RUnlock()
	if collection.items == nil {
		return orderedset.NewSliceSet(0)
	}
	context := QueryContext{
		collection: collection,
		setMaker:   collection.QuerySetMaker,
	}
	return query.Query(context)
}
