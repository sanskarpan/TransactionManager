package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
)

// RecordType identifies the kind of WAL record.
type RecordType uint8

const (
	RecordBegin           RecordType = 0x01
	RecordWrite           RecordType = 0x02
	RecordCommit          RecordType = 0x03
	RecordAbort           RecordType = 0x04
	RecordCLR             RecordType = 0x05 // Compensation Log Record
	RecordCheckpointBegin RecordType = 0x06
	RecordCheckpointEnd   RecordType = 0x07
)

// ATTEntry is a single row of the Active Transaction Table captured at
// checkpoint time.
type ATTEntry struct {
	LastLSN  LSN
	FirstLSN LSN
	Status   uint8 // 0=active, 1=committed, 2=aborted
}

// LogRecord is the in-memory representation of a WAL record.
type LogRecord struct {
	LSN     LSN
	Type    RecordType
	TxnID   uint64
	PrevLSN LSN // previous record for same txn (chain for undo)

	// For RecordWrite and RecordCLR:
	UndoNextLSN LSN   // for CLR: next LSN to undo in this txn; for Write: same as PrevLSN
	Op          uint8 // UndoInsert=0, UndoUpdate=1, UndoDelete=2
	Table       string
	Key         string
	Before      []byte // EncodeValues(beforeImage)
	After       []byte // EncodeValues(afterImage)

	// For RecordCheckpointBegin:
	ATT map[uint64]ATTEntry // active transaction table

	// For RecordCheckpointEnd:
	BeginLSN LSN

	// For RecordBegin:
	Timestamp int64 // UnixNano
}

// On-disk layout:
//
//	[TotalLen: uint32 LE] — total bytes of the record including this header
//	[CRC32:    uint32 LE] — CRC32 of everything after this field (RecordType onward)
//	[RecordType: uint8]
//	[LSN: uint64 LE]
//	[TxnID: uint64 LE]
//	[PrevLSN: uint64 LE]
//	[body: variable, type-specific]
const (
	hdrTotalLen = 4
	hdrCRC      = 4
	hdrFixed    = hdrTotalLen + hdrCRC + 1 + 8 + 8 + 8 // TotalLen+CRC+Type+LSN+TxnID+PrevLSN
)

func encodeRecord(rec LogRecord) []byte {
	var body bytes.Buffer

	switch rec.Type {
	case RecordBegin:
		var ts [8]byte
		binary.LittleEndian.PutUint64(ts[:], uint64(rec.Timestamp))
		body.Write(ts[:])

	case RecordWrite:
		var h [8 + 1 + 2 + 2 + 4 + 4]byte
		binary.LittleEndian.PutUint64(h[0:8], rec.UndoNextLSN)
		h[8] = rec.Op
		binary.LittleEndian.PutUint16(h[9:11], uint16(len(rec.Table)))
		binary.LittleEndian.PutUint16(h[11:13], uint16(len(rec.Key)))
		binary.LittleEndian.PutUint32(h[13:17], uint32(len(rec.Before)))
		binary.LittleEndian.PutUint32(h[17:21], uint32(len(rec.After)))
		body.Write(h[:])
		body.WriteString(rec.Table)
		body.WriteString(rec.Key)
		body.Write(rec.Before)
		body.Write(rec.After)

	case RecordCommit, RecordAbort:
		var last [8]byte
		binary.LittleEndian.PutUint64(last[:], rec.PrevLSN)
		body.Write(last[:])

	case RecordCLR:
		var u [8]byte
		binary.LittleEndian.PutUint64(u[:], rec.UndoNextLSN)
		body.Write(u[:])

	case RecordCheckpointBegin:
		var gobBuf bytes.Buffer
		enc := gob.NewEncoder(&gobBuf)
		_ = enc.Encode(rec.ATT)
		gobBytes := gobBuf.Bytes()
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(gobBytes)))
		body.Write(lenBuf[:])
		body.Write(gobBytes)

	case RecordCheckpointEnd:
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], rec.BeginLSN)
		body.Write(b[:])
	}

	bodyBytes := body.Bytes()

	// Build the payload that gets CRC'd: Type + LSN + TxnID + PrevLSN + body
	payloadLen := 1 + 8 + 8 + 8 + len(bodyBytes)
	total := hdrTotalLen + hdrCRC + payloadLen

	out := make([]byte, total)
	binary.LittleEndian.PutUint32(out[0:4], uint32(total))

	// Fill payload (starting at offset 8, after TotalLen+CRC)
	out[8] = uint8(rec.Type)
	binary.LittleEndian.PutUint64(out[9:17], rec.LSN)
	binary.LittleEndian.PutUint64(out[17:25], rec.TxnID)
	binary.LittleEndian.PutUint64(out[25:33], rec.PrevLSN)
	copy(out[33:], bodyBytes)

	// CRC covers everything from offset 8 onward
	crc := crc32.ChecksumIEEE(out[8:])
	binary.LittleEndian.PutUint32(out[4:8], crc)

	return out
}

func decodeRecord(b []byte) (LogRecord, error) {
	if len(b) < hdrFixed {
		return LogRecord{}, fmt.Errorf("wal: record too short (%d bytes)", len(b))
	}

	totalLen := binary.LittleEndian.Uint32(b[0:4])
	if int(totalLen) > len(b) {
		return LogRecord{}, fmt.Errorf("wal: record claims %d bytes but only %d available", totalLen, len(b))
	}

	storedCRC := binary.LittleEndian.Uint32(b[4:8])
	actualCRC := crc32.ChecksumIEEE(b[8:totalLen])
	if storedCRC != actualCRC {
		return LogRecord{}, fmt.Errorf("wal: CRC mismatch (stored 0x%08x, computed 0x%08x)", storedCRC, actualCRC)
	}

	rec := LogRecord{}
	rec.Type = RecordType(b[8])
	rec.LSN = binary.LittleEndian.Uint64(b[9:17])
	rec.TxnID = binary.LittleEndian.Uint64(b[17:25])
	rec.PrevLSN = binary.LittleEndian.Uint64(b[25:33])

	body := b[33:totalLen]

	switch rec.Type {
	case RecordBegin:
		if len(body) < 8 {
			return LogRecord{}, fmt.Errorf("wal: BEGIN body too short")
		}
		rec.Timestamp = int64(binary.LittleEndian.Uint64(body[0:8]))

	case RecordWrite:
		if len(body) < 21 {
			return LogRecord{}, fmt.Errorf("wal: WRITE body too short")
		}
		rec.UndoNextLSN = binary.LittleEndian.Uint64(body[0:8])
		rec.Op = body[8]
		tableLen := int(binary.LittleEndian.Uint16(body[9:11]))
		keyLen := int(binary.LittleEndian.Uint16(body[11:13]))
		beforeLen := int(binary.LittleEndian.Uint32(body[13:17]))
		afterLen := int(binary.LittleEndian.Uint32(body[17:21]))
		pos := 21
		if pos+tableLen+keyLen+beforeLen+afterLen > len(body) {
			return LogRecord{}, fmt.Errorf("wal: WRITE body payload truncated")
		}
		rec.Table = string(body[pos : pos+tableLen])
		pos += tableLen
		rec.Key = string(body[pos : pos+keyLen])
		pos += keyLen
		rec.Before = make([]byte, beforeLen)
		copy(rec.Before, body[pos:pos+beforeLen])
		pos += beforeLen
		rec.After = make([]byte, afterLen)
		copy(rec.After, body[pos:pos+afterLen])

	case RecordCommit, RecordAbort:
		if len(body) < 8 {
			return LogRecord{}, fmt.Errorf("wal: COMMIT/ABORT body too short")
		}
		// LastLSN is stored but same as PrevLSN for commit/abort
		_ = binary.LittleEndian.Uint64(body[0:8])

	case RecordCLR:
		if len(body) < 8 {
			return LogRecord{}, fmt.Errorf("wal: CLR body too short")
		}
		rec.UndoNextLSN = binary.LittleEndian.Uint64(body[0:8])

	case RecordCheckpointBegin:
		if len(body) < 4 {
			return LogRecord{}, fmt.Errorf("wal: CHECKPOINT_BEGIN body too short")
		}
		gobLen := int(binary.LittleEndian.Uint32(body[0:4]))
		if 4+gobLen > len(body) {
			return LogRecord{}, fmt.Errorf("wal: CHECKPOINT_BEGIN gob payload truncated")
		}
		dec := gob.NewDecoder(bytes.NewReader(body[4 : 4+gobLen]))
		rec.ATT = make(map[uint64]ATTEntry)
		if err := dec.Decode(&rec.ATT); err != nil {
			return LogRecord{}, fmt.Errorf("wal: CHECKPOINT_BEGIN gob decode: %w", err)
		}

	case RecordCheckpointEnd:
		if len(body) < 8 {
			return LogRecord{}, fmt.Errorf("wal: CHECKPOINT_END body too short")
		}
		rec.BeginLSN = binary.LittleEndian.Uint64(body[0:8])

	default:
		return LogRecord{}, fmt.Errorf("wal: unknown record type 0x%02x", rec.Type)
	}

	return rec, nil
}
