package odb

import (
	"github.com/yangchenxing/go-orderedset"
)

type QueryContext struct {
	collection *Collection
	setMaker   func() orderedset.OrderedSet
}

func (c QueryContext) makeEmptySet() orderedset.OrderedSet {
	if c.setMaker == nil {
		return orderedset.NewListSet()
	}
	return c.setMaker()
}

type Query interface {
	Query(QueryContext) orderedset.OrderedSet
}

type IntersectionQuery []Query

func (query IntersectionQuery) Query(context QueryContext) orderedset.OrderedSet {
	res := query[0].Query(context)
	for _, subquery := range query[1:] {
		temp := context.makeEmptySet()
		orderedset.Intersect(res, subquery.Query(context), temp)
		res = temp
	}
	return res
}

type UnionQuery []Query

func (query UnionQuery) Query(context QueryContext) orderedset.OrderedSet {
	res := query[0].Query(context)
	for _, subquery := range query[1:] {
		temp := context.makeEmptySet()
		orderedset.Union(res, subquery.Query(context), temp)
		res = temp
	}
	return res
}

type ComplementQuery []Query

func (query ComplementQuery) Query(context QueryContext) orderedset.OrderedSet {
	res := query[0].Query(context)
	for _, subquery := range query[1:] {
		temp := context.makeEmptySet()
		orderedset.Complement(res, subquery.Query(context), temp)
		res = temp
	}
	return res
}

type IntIndexQuery struct {
	key   string
	value int64
}

func (query IntIndexQuery) Query(context QueryContext) orderedset.OrderedSet {

}
