package lock

import (
	"fmt"
	"sort"
	"strings"
)

type queue struct {
	name string
	data []*lock
}

func newQueue(name string, pending []*lock) *queue {
	sort.SliceStable(pending, func(i, j int) bool {
		return pending[i].Less(pending[j])
	})
	return &queue{name: name, data: pending}
}

func (q *queue) entryCopy() []*lock {
	locks := make([]*lock, len(q.data))
	copy(locks, q.data)
	return locks
}

func (q *queue) Requeue(l *lock) {
	q.Remove(l)
	q.Add(l)
}

func (q *queue) Add(l *lock) {
	// insert to pending in sorted order. Usually append to end
	idxInsert := len(q.data)
	for ; idxInsert > 0; idxInsert-- {
		if q.data[idxInsert-1].Less(l) {
			if idxInsert == len(q.data) {
				q.data = append(q.data, l)
			} else {
				q.data = append(q.data[:idxInsert], append([]*lock{l}, q.data[idxInsert:]...)...)
			}
			break
		}
	}
	if idxInsert == 0 {
		q.data = append([]*lock{l}, q.data...)
	}
}

func (q *queue) Remove(l *lock) {
	ri := -1
	for i, val := range q.data {
		if val.Name == l.Name {
			ri = i
			break
		}
	}
	if ri != -1 {
		q.data = append(q.data[:ri], q.data[ri+1:]...)
	}
}

func (q *queue) String() string {
	names := make([]string, len(q.data))
	for i, l := range q.data {
		names[i] = fmt.Sprintf("%s:%d", l.Name, l.Priority)
	}
	return "queue/" + q.name + ":[" + strings.Join(names, ",") + "]"
}
