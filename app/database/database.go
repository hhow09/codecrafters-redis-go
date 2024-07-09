package database

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	NO_EXPIRY = 0
)

type DB struct {
	datas           map[string]*Data
	mu              sync.RWMutex
	streamEntrySubs map[string][]subscription
	subMU           sync.RWMutex
}

type Data struct {
	Type              string
	Value             string
	ExpireTimestampMS uint64
	Entries           []Entry
}

type Entry struct {
	Ts  uint64
	Seq uint64
	KVs []KeyValue
}

type KeyValue struct {
	Key   string
	Value string
}

func NewString(value string, expireTimestampMS uint64) *Data {
	return &Data{
		Type:              TypeString,
		Value:             value,
		ExpireTimestampMS: expireTimestampMS,
	}
}

func NewDB() *DB {
	return &DB{
		datas:           make(map[string]*Data),
		streamEntrySubs: make(map[string][]subscription),
	}
}

func NewFromLoad(datas map[string]*Data) *DB {
	if datas == nil {
		return NewDB()
	}
	return &DB{
		datas: datas,
	}
}

func (d *DB) Get(key string) string {
	return d.get(key).Value
}

func (d *DB) get(key string) Data {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if data, ok := d.datas[key]; ok {
		if data.ExpireTimestampMS != NO_EXPIRY && uint64(time.Now().UnixMilli()) > data.ExpireTimestampMS {
			delete(d.datas, key)
			return Data{}
		}
		return *data
	}
	return Data{}
}

func (d *DB) Type(key string) string {
	return d.get(key).Type
}

func (d *DB) Set(key, value string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.datas[key] = NewString(value, NO_EXPIRY)
}

func (d *DB) SetExp(key, value string, exp int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.datas[key] = NewString(value, uint64(exp))
}

func (d *DB) Keys(reg *regexp.Regexp) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	keys := make([]string, 0)
	for key := range d.datas {
		if reg.MatchString(key) {
			keys = append(keys, key)
		}
	}
	return keys
}

func (d *DB) XAdd(key, inputEntryID string, kvs []KeyValue) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if data, ok := d.datas[key]; ok {
		if data.Type != TypeStream {
			return "", ErrWrongType
		}
		ts, seq, err := validateAndGenerateEntryID(inputEntryID, &data.Entries[len(data.Entries)-1])
		if err != nil {
			return "", err
		}
		ent := Entry{
			Ts:  ts,
			Seq: seq,
			KVs: kvs,
		}
		data.Entries = append(data.Entries, ent)
		d.publishXAdd(key, ent)
		return StreamEntryID(ts, seq), nil
	}
	ts, seq, err := validateAndGenerateEntryID(inputEntryID, nil)
	if err != nil {
		return "", err
	}
	ent := Entry{
		Ts:  ts,
		Seq: seq,
		KVs: kvs,
	}
	d.datas[key] = &Data{
		Type:    TypeStream,
		Entries: []Entry{ent},
	}
	d.publishXAdd(key, ent)
	return StreamEntryID(ts, seq), nil
}

func validateAndGenerateEntryID(entryID string, lastEntry *Entry) (uint64, uint64, error) {
	if entryID == "" {
		return 0, 0, ErrInvalidEntryID
	}
	var timeStamp uint64
	var seq uint64
	var err error
	autoGenSeq := false
	if entryID == "*" {
		// fully auto generate entryID
		timeStamp = uint64(time.Now().UnixMilli())
		autoGenSeq = true
	} else {
		timeStamp, seq, autoGenSeq, err = parseEntryIDForXAdd(entryID)
		if err != nil {
			return 0, 0, err
		}
	}
	if autoGenSeq {
		if lastEntry != nil && timeStamp == lastEntry.Ts {
			return timeStamp, lastEntry.Seq + 1, nil
		} else {
			if timeStamp == 0 {
				return timeStamp, 1, nil
			}
			return timeStamp, 0, nil
		}
	}
	if lastEntry == nil {
		return timeStamp, seq, nil
	}
	if timeStamp < lastEntry.Ts {
		return 0, 0, ErrIDTooSmall
	}
	if timeStamp == lastEntry.Ts && seq <= lastEntry.Seq {
		return 0, 0, ErrIDTooSmall
	}
	return timeStamp, seq, nil
}

func parseEntryIDForXAdd(entryID string) (uint64, uint64, bool, error) {
	arr := strings.Split(entryID, "-")
	if len(arr) != 2 {
		return 0, 0, false, ErrInvalidEntryID
	}
	timeStamp, err := strconv.ParseUint(arr[0], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("%v: %v", ErrInvalidEntryID, err)
	}
	if arr[1] == "*" {
		return timeStamp, 0, true, nil
	}
	seq, err := strconv.ParseUint(arr[1], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("%v: %v", ErrInvalidEntryID, err)
	}
	if seq == 0 && timeStamp == 0 {
		return 0, 0, false, ErrIDMinVal
	}
	return timeStamp, seq, false, nil
}

func (d *DB) Xrange(key, start, end string) ([]Entry, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var sts, sseq, ets, eseq uint64
	var notSpecified bool
	var err error
	if start == "-" {
		sts = 0
		sseq = 0
	} else {
		sts, sseq, notSpecified, err = parseEntryIDForXRange(start)
		if err != nil {
			return nil, fmt.Errorf("invalid start: %s , err: %w", start, err)
		}
		if notSpecified {
			sseq = 0
		}
	}
	if end == "+" {
		ets = math.MaxUint64
		eseq = math.MaxUint64
	} else {
		ets, eseq, notSpecified, err = parseEntryIDForXRange(end)
		if err != nil {
			return nil, fmt.Errorf("invalid start: %s , err: %w", start, err)
		}
		if notSpecified {
			eseq = math.MaxUint64
		}

	}
	if data, ok := d.datas[key]; ok {
		if data.Type != TypeStream {
			return nil, ErrWrongType
		}
		startIdx := biSectLeft(data.Entries, sts, sseq)
		endIdx := biSectLeft(data.Entries, ets, eseq+1)
		if startIdx > endIdx {
			return nil, nil
		}
		return data.Entries[startIdx:endIdx], nil
	}
	return nil, nil
}

// return (start, end, not specified, error)
func parseEntryIDForXRange(entryID string) (uint64, uint64, bool, error) {
	arr := strings.Split(entryID, "-")
	if len(arr) > 2 {
		return 0, 0, false, ErrInvalidEntryID
	}
	timeStamp, err := strconv.ParseUint(arr[0], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("%v: %v", ErrInvalidEntryID, err)
	}
	if len(arr) == 1 {
		return timeStamp, 0, true, nil
	}
	seq, err := strconv.ParseUint(arr[1], 10, 64)
	if err != nil {
		return 0, 0, false, fmt.Errorf("%v: %v", ErrInvalidEntryID, err)
	}
	return timeStamp, seq, false, nil
}

func biSectLeft(entries []Entry, ts, seq uint64) int {
	low, high := 0, len(entries)
	for low < high {
		mid := low + (high-low)/2
		if (entries[mid].Ts < ts) || (entries[mid].Ts == ts && entries[mid].Seq < seq) {
			low = mid + 1
		} else {
			high = mid
		}
	}

	return low
}

const (
	NO_BLOCKING         time.Duration = -1
	BLOCKING_NO_TIMEOUT time.Duration = 0
)

func (d *DB) Xread(key, start string, blocking time.Duration) ([]Entry, chan Entry, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var sts, sseq uint64
	var err error
	var useLatest bool // blocking XREAD command signals that we only want new entries. This is similar to passing in the maximum ID we currently have in the stream.
	if start == "$" {
		useLatest = true
	} else {
		useLatest = false
		sts, sseq, _, err = parseEntryIDForXRange(start)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid start: %s , err: %w", start, err)
		}
	}
	if blocking > 0 {
		d.mu.RUnlock()
		time.Sleep(blocking)
		d.mu.RLock()
	}
	if data, ok := d.datas[key]; ok {
		if data.Type != TypeStream {
			return nil, nil, ErrWrongType
		}
		if useLatest {
			ch, err := d.subscribeXAdd(key, sts, sseq)
			if err != nil {
				return nil, nil, err
			}
			return nil, ch, nil
		}
		startIdx := biSectLeft(data.Entries, sts, sseq+1) // exclusive
		if len(data.Entries[startIdx:]) == 0 && blocking == BLOCKING_NO_TIMEOUT {
			ch, err := d.subscribeXAdd(key, sts, sseq)
			if err != nil {
				return nil, nil, err
			}
			return nil, ch, nil
		}
		return data.Entries[startIdx:], nil, nil
	} else if blocking == BLOCKING_NO_TIMEOUT {
		ch, err := d.subscribeXAdd(key, sts, sseq)
		if err != nil {
			return nil, nil, err
		}
		return nil, ch, nil
	}
	return nil, nil, nil
}

type subscription struct {
	Chan     chan Entry
	EntryTs  uint64
	EntrySeq uint64
}

// use with publishXAdd
func (d *DB) subscribeXAdd(key string, entryTS uint64, entrySeq uint64) (chan Entry, error) {
	d.subMU.Lock()
	defer d.subMU.Unlock()
	sub := subscription{
		Chan:     make(chan Entry),
		EntryTs:  entryTS,
		EntrySeq: entrySeq,
	}
	if subs, ok := d.streamEntrySubs[key]; ok {
		d.streamEntrySubs[key] = append(subs, sub)
		// binary search to find the index to insert
		sort.Slice(d.streamEntrySubs[key], func(i, j int) bool {
			if d.streamEntrySubs[key][i].EntryTs == d.streamEntrySubs[key][j].EntryTs {
				return d.streamEntrySubs[key][i].EntrySeq < d.streamEntrySubs[key][j].EntrySeq
			}
			return d.streamEntrySubs[key][i].EntryTs < d.streamEntrySubs[key][j].EntryTs
		})
	} else {
		d.streamEntrySubs[key] = []subscription{sub}
	}
	return sub.Chan, nil
}

// publishXAdd publishes the entry to suitable subscribers of the key
func (d *DB) publishXAdd(key string, entry Entry) {
	d.subMU.RLock()
	defer d.subMU.RUnlock()
	if subs, ok := d.streamEntrySubs[key]; ok {
		i := 0
		for i < len(subs) && (entry.Ts > subs[i].EntryTs || (entry.Ts == subs[i].EntryTs && entry.Seq > subs[i].EntrySeq)) {
			subs[i].Chan <- entry
			close(subs[i].Chan)
			i++
		}
		d.streamEntrySubs[key] = subs[i:]
	}
}
