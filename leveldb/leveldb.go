package leveldb
// #cgo LDFLAGS: -lleveldb
// #include <stdlib.h>
// #include "leveldb/include/leveldb/c.h"
import "C"

import (
	"errors"
	"unsafe"
)

// C Level pointer holder
type LevelDB struct {
	CLevelDB *C.leveldb_t
	Name     string
	ReadOpt  *C.leveldb_readoptions_t
	WriteOpt  *C.leveldb_readoptions_t
}

type LevelDBIter struct {
	Iter *C.leveldb_iterator_t
}

func (iter * LevelDBIter ) Destroy(){
	if iter.Iter==nil {
		return ;
	}
	C.leveldb_iter_destroy(iter.Iter);
	iter.Iter=nil;
}

func (iter * LevelDBIter ) Valid() bool{
	return C.leveldb_iter_valid(iter.Iter)==1
}
func (iter * LevelDBIter ) SeekToFirst() {
	C.leveldb_iter_seek_to_first(iter.Iter);
}
func (iter * LevelDBIter ) SeekToLast() {
	C.leveldb_iter_seek_to_last(iter.Iter);
}
func (iter * LevelDBIter ) Next() {
	C.leveldb_iter_next(iter.Iter);
}
func (iter * LevelDBIter ) Prev() {
	C.leveldb_iter_prev(iter.Iter);
}

func (iter * LevelDBIter ) Seek(key string) {
	k := C.CString(key) // copy
	defer C.leveldb_free(unsafe.Pointer(k))
	C.leveldb_iter_seek(iter.Iter,k,C.size_t(len(key)));
}

func (iter *LevelDBIter) Key() string {
	var vallen C.size_t
	cvalue := C.leveldb_iter_key(iter.Iter, &vallen)
	if cvalue==nil {
		return ""
	}
	return C.GoStringN(cvalue,C.int(vallen));
}
func (iter *LevelDBIter) Value() string {
	var vallen C.size_t
	cvalue := C.leveldb_iter_value(iter.Iter, &vallen)
	if cvalue==nil {
		return "";
	}
	return C.GoStringN(cvalue,C.int(vallen));
}
func (iter *LevelDBIter) Error() string {
	var cerr *C.char
	C.leveldb_iter_get_error(iter.Iter, &cerr)
	defer C.leveldb_free(unsafe.Pointer(cerr))
	return C.GoString(cerr)
}


// Open LevelDB with given name
func OpenLevelDB(path string) (leveldb *LevelDB, err error) {

	cpath := C.CString(path) // convert path to c string
	defer C.leveldb_free(unsafe.Pointer(cpath))

	// allocate LevelDB Option struct to open
	opt := C.leveldb_options_create()
	defer C.leveldb_free(unsafe.Pointer(opt))
	cache:=C.leveldb_cache_create_lru(300*1024*1024);
	// set open option
	C.leveldb_options_set_create_if_missing(opt, C.uchar(1))
	C.leveldb_options_set_info_log(opt,nil);
	C.leveldb_options_set_write_buffer_size(opt,30*1024*1024);
	C.leveldb_options_set_block_size(opt,30*1024*1024);
	C.leveldb_options_set_compression(opt,C.leveldb_snappy_compression);
	C.leveldb_options_set_cache(opt,cache);

	// open leveldb
	var cerr *C.char
	cleveldb := C.leveldb_open(opt, cpath, &cerr)

	if cerr != nil {
		defer C.leveldb_free(unsafe.Pointer(cerr))
		return nil, errors.New(C.GoString(cerr))
	}

	return &LevelDB{CLevelDB:cleveldb, Name:path,ReadOpt:C.leveldb_readoptions_create(),WriteOpt:C.leveldb_writeoptions_create()}, nil
}
func (db *LevelDB) Iter() *LevelDBIter {
	iter:=C.leveldb_create_iterator(db.CLevelDB,db.ReadOpt)
	return &LevelDBIter{iter}
}

// Close the database
func (db *LevelDB) Close() {
	C.leveldb_close(db.CLevelDB)
}

// Put key, value to database
func (db *LevelDB) Put(key, value string) (err error) {

	opt := C.leveldb_writeoptions_create() // write option
	defer C.leveldb_free(unsafe.Pointer(opt))

	k := C.CString(key) // copy
	defer C.leveldb_free(unsafe.Pointer(k))

	v := C.CString(value)
	defer C.leveldb_free(unsafe.Pointer(v))

	var cerr *C.char
	C.leveldb_put(db.CLevelDB, opt, k, C.size_t(len(key)), v, C.size_t(len(value)), &cerr)

	if cerr != nil {
		defer C.leveldb_free(unsafe.Pointer(cerr))
		return errors.New(C.GoString(cerr))
	}

	return
}

func (db *LevelDB) Get(key string) (value string, err error) {

	opt := C.leveldb_readoptions_create() // write option
	defer C.leveldb_free(unsafe.Pointer(opt))

	k := C.CString(key) // copy
	defer C.leveldb_free(unsafe.Pointer(k))

	var vallen C.size_t
	var cerr *C.char
	cvalue := C.leveldb_get(db.CLevelDB, opt, k, C.size_t(len(key)), &vallen, &cerr)

	if cerr != nil {
		defer C.leveldb_free(unsafe.Pointer(cerr))
		return "", errors.New(C.GoString(cerr))
	}

	if cvalue == nil {
		return "", nil
	}

	defer C.leveldb_free(unsafe.Pointer(cvalue))
	return C.GoStringN(cvalue,C.int(vallen)), nil
}

func (db *LevelDB) Delete(key string) (err error) {

	k := C.CString(key) // copy
	defer C.leveldb_free(unsafe.Pointer(k))

	var cerr *C.char
	C.leveldb_delete(db.CLevelDB, db.WriteOpt, k, C.size_t(len(key)), &cerr)

	if cerr != nil {
		defer C.leveldb_free(unsafe.Pointer(cerr))
		return errors.New(C.GoString(cerr))
	}

	return
}
