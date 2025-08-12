package db

import (
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/internal/exchange"
	"github.com/codecrafters-io/redis-starter-go/app/internal/utils"
)

type DB struct {
	Store       *Store
	Replication *Replication
	PubSub      *exchange.PubSub
	List        map[string][]string
	Role        string
	RDBFileDir  string
	RDBFileName string
}

func New(role string) *DB {
	return &DB{
		Store:       &Store{Data: make(map[string]cacheValue), Streams: make(map[string][]StreamEntry)},
		Replication: &Replication{ID: utils.GenerateReplicaID(), Offset: 0, Replicas: make([]*ReplicaConn, 0), NumAcksRecieved: 0},
		PubSub:      exchange.NewPubSub(),
		Role:        role,
		List:        make(map[string][]string),
	}
}

func (db *DB) RPush(key string, elements []string) int {
	if _, ok := db.List[key]; !ok {
		db.List[key] = make([]string, 0)
	}

	db.List[key] = append(db.List[key], elements...)
	return len(db.List[key])

}

func (db *DB) ParseAndLoadRDBFile() error {
	_, err := os.Stat(filepath.Join(db.RDBFileDir, db.RDBFileName))
	if os.IsNotExist(err) {
		fmt.Println("RDB file not found, starting with empty database.")
		return nil
	} else if err != nil {
		return fmt.Errorf("error checking RDB file status: %w", err)
	}

	data, err := ParseRDBFile(db.RDBFileDir, db.RDBFileName)
	if err != nil {
		return fmt.Errorf("failed to parse RDB file: %w", err)
	}

	db.Store.Mu.Lock()
	defer db.Store.Mu.Unlock()
	db.Store.Data = data
	return nil
}

func (db *DB) UpdateOffset(length int) {
	db.Replication.Offset = db.Replication.Offset + length
}

func (db *DB) AddReplica(conn net.Conn) {
	db.Replication.ReplicaMu.Lock()
	defer db.Replication.ReplicaMu.Unlock()

	db.Replication.Replicas = append(db.Replication.Replicas, &ReplicaConn{Conn: conn})
	fmt.Printf("Added replica connection. Total replicas: %d \n", len(db.Replication.Replicas))
}

func (db *DB) RemoveReplica(conn net.Conn) {
	db.Replication.ReplicaMu.Lock()
	defer db.Replication.ReplicaMu.Unlock()
	for i, r := range db.Replication.Replicas {
		if r.Conn == conn {
			fmt.Printf("Removing replica connection from address: %s\n", conn.RemoteAddr().String())
			_ = r.Conn.Close()
			db.Replication.Replicas = append(db.Replication.Replicas[:i], db.Replication.Replicas[i+1:]...)
			break
		}
	}
}

// PropagateCommand now locks per-replica before writing
func (db *DB) PropagateCommand(args []string) {
	db.Replication.ReplicaMu.RLock()
	defer db.Replication.ReplicaMu.RUnlock()

	respCmd := utils.FormatRESPArray(args)

	for _, r := range db.Replication.Replicas {
		r.Mu.Lock()
		_, err := r.Conn.Write([]byte(respCmd))
		r.Mu.Unlock()
		if err != nil {
			fmt.Printf("Failed to propagate command to replica: %v\n", err)
			// Note: we don't remove here; removal happens in other code paths (or optionally add removal logic)
			continue
		}
	}
}

func (db *DB) GetLastID(key string) string {
	db.Store.Mu.RLock()
	defer db.Store.Mu.RUnlock()

	if stream, ok := db.Store.Streams[key]; ok && len(stream) > 0 {
		return stream[len(stream)-1].ID
	}
	return "0-0"
}

func (db *DB) Get(key string) (string, bool) {
	db.Store.Mu.RLock()
	defer db.Store.Mu.RUnlock()

	val, ok := db.Store.Data[key]
	if !ok {
		return "", false
	}

	if val.Ttl > 0 && time.Now().UnixMilli() > val.Ttl {

		delete(db.Store.Data, key)
		return "", false
	}
	return val.Value, true
}

func (db *DB) GetType(key string) string {
	db.Store.Mu.RLock()
	defer db.Store.Mu.RUnlock()

	if _, ok := db.Store.Data[key]; ok {
		return "string"
	}
	if _, ok := db.Store.Streams[key]; ok {
		return "stream"
	}
	return "none"
}
func (db *DB) Set(key, Value string, ttlMilSec int64) {
	db.Store.Mu.Lock()
	defer db.Store.Mu.Unlock()

	var Ttl int64
	if ttlMilSec > 0 {
		Ttl = time.Now().UnixMilli() + ttlMilSec
	}
	db.Store.Data[key] = cacheValue{Value: Value, Ttl: Ttl}
}

func (db *DB) XAdd(key, ID string, fields map[string]string) (string, error) {
	db.Store.Mu.Lock()
	defer db.Store.Mu.Unlock()

	if _, ok := db.Store.Data[key]; ok {
		return "", fmt.Errorf("wrong key type")
	}

	lastID := ""
	if stream, streamExists := db.Store.Streams[key]; streamExists && len(stream) > 0 {
		lastID = stream[len(stream)-1].ID
	}

	finalID, err := utils.ValidateStreamID(ID, lastID)
	if err != nil {
		return "", err
	}

	entry := StreamEntry{
		ID:     finalID,
		Fields: fields,
	}

	db.Store.Streams[key] = append(db.Store.Streams[key], entry)
	return finalID, nil
}

func (db *DB) XRange(key, start, end string) []StreamEntry {
	entries, ok := db.Store.Streams[key]
	if !ok {
		return nil
	}

	startMs, startSeq := utils.ParsID(start) // (if start == "-"") strings.split will split it into 0,0
	var endMs, endSeq int64
	if end == "+" {
		endMs = math.MaxInt64
		endSeq = math.MaxInt64
	} else {
		endMs, endSeq = utils.ParsID(end)
	}
	var wantedEntries []StreamEntry

	for _, entry := range entries {
		entryMs, entrySeq := utils.ParsID(entry.ID)
		if utils.CompareIDs(entryMs, entrySeq, startMs, startSeq) >= 0 &&
			utils.CompareIDs(entryMs, entrySeq, endMs, endSeq) <= 0 {
			wantedEntries = append(wantedEntries, entry)
		}
	}

	return wantedEntries
}

func (db *DB) XREAD(key, ID string) []StreamEntry {
	entries, ok := db.Store.Streams[key]
	if !ok {
		return nil
	}
	IDMs, IDSeq := utils.ParsID(ID)
	var wantedEntries []StreamEntry

	for _, entry := range entries {
		entryMs, entrySeq := utils.ParsID(entry.ID)
		if utils.CompareIDs(entryMs, entrySeq, IDMs, IDSeq) > 0 {
			wantedEntries = append(wantedEntries, entry)
		}
	}

	return wantedEntries
}

func (db *DB) INCR(key string) int64 {
	db.Store.Mu.Lock()
	defer db.Store.Mu.Unlock()

	val, ok := db.Store.Data[key]
	if !ok {
		db.Store.Data[key] = cacheValue{Value: "1", Ttl: 0}
		return int64(1)
	}

	intVal, err := strconv.ParseInt(val.Value, 10, 64)
	if err != nil {
		return -1
	}
	val.Value = strconv.Itoa(int(intVal + 1))
	db.Store.Data[key] = val
	return intVal + 1
}
