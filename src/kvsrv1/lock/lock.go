package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name string
	id   string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.name = lockname
	lk.id = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		result, version, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey || result == "" || result == lk.id {
			temp_err := lk.ck.Put(lk.name, lk.id, version)
			if temp_err == rpc.OK {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	result, version, err := lk.ck.Get(lk.name)
	if err != rpc.ErrNoKey && result != "" && result == lk.id {
		lk.ck.Put(lk.name, "", version)
	}
}
