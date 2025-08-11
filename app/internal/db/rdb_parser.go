package db

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)


func ParseRDBFile(dir, filename string) (map[string]cacheValue, error) {
	filePath := filepath.Join(dir, filename)

	// Verify file exists; if not, start with an empty DB.
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Printf("RDB file not found at %s. Starting with empty DB.\n", filePath)
		return make(map[string]cacheValue), nil
	} else if err != nil {
		return nil, fmt.Errorf("error checking RDB file: %w", err)
	}

	// Open the file.
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening RDB file: %w", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	// Verify magic string.
	magic := make([]byte, 5)
	if _, err := io.ReadFull(reader, magic); err != nil || string(magic) != "REDIS" {
		return nil, fmt.Errorf("invalid RDB magic number")
	}

	// Verify version (4 ASCII digits).
	version := make([]byte, 4)
	if _, err := io.ReadFull(reader, version); err != nil {
		return nil, fmt.Errorf("invalid RDB version")
	}
	_ = version // version is currently unused.

	data := make(map[string]cacheValue)
	var ttl int64 // TTL for the next key (in ms), reset after each key.

	for {
		opcode, err := reader.ReadByte()
		if err == io.EOF {
			break // Normal termination.
		}
		if err != nil {
			return nil, fmt.Errorf("error reading opcode: %w", err)
		}

		switch opcode {
		case 0xFE: // SELECT DB (ignored for now)
			if _, err := readLength(reader); err != nil {
				return nil, fmt.Errorf("error reading DB selector: %w", err)
			}
		case 0xFA: // AUX key/value (ignored)
			if _, err := readString(reader); err != nil {
				return nil, fmt.Errorf("error reading AUX key: %w", err)
			}
			if _, err := readString(reader); err != nil {
				return nil, fmt.Errorf("error reading AUX value: %w", err)
			}
		case 0xFD: // Expiry in seconds (convert to ms).
			var buf [4]byte
			if _, err := io.ReadFull(reader, buf[:]); err != nil {
				return nil, fmt.Errorf("error reading expiry seconds: %w", err)
			}
			ttl = int64(binary.BigEndian.Uint32(buf[:])) * 1000
		case 0xFC: // Expiry in milliseconds.
			var buf [8]byte
			if _, err := io.ReadFull(reader, buf[:]); err != nil {
				return nil, fmt.Errorf("error reading expiry ms: %w", err)
			}
			ttl = int64(binary.BigEndian.Uint64(buf[:]))
		case 0x00: // String value.
			key, err := readString(reader)
			if err != nil {
				return nil, fmt.Errorf("error reading key: %w", err)
			}
			value, err := readString(reader)
			if err != nil {
				return nil, fmt.Errorf("error reading value for key %s: %w", key, err)
			}
			data[key] = cacheValue{Value: value, Ttl: ttl}
			ttl = 0 // Reset for the next key.
		case 0xFF: // End of RDB.
			return data, nil
		}
	}
	// If we reach here the file ended unexpectedly.
	return data, fmt.Errorf("unexpected EOF while parsing RDB")
}

// readLength reads a length‑encoded integer according to the
// RDB format specification.
func readLength(r *bufio.Reader) (int, error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch (first & 0xC0) >> 6 {
	case 0: // 6‑bit length
		return int(first & 0x3F), nil
	case 1: // 14‑bit length
		second, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		return ((int(first) & 0x3F) << 8) | int(second), nil
	case 2: // 32‑bit length
		var buf [4]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, err
		}
		return int(binary.LittleEndian.Uint32(buf[:])), nil
	case 3: // special encoding (e.g. integer)
		return int(first & 0x3F), nil
	default:
		return 0, fmt.Errorf("invalid length encoding")
	}
}

// readString reads a bulk string or integer (special encoding)
// from the RDB file.
func readString(r *bufio.Reader) (string, error) {
	// Peek the first byte to decide whether this is a special encoding.
	first, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	encodingType := (first & 0xC0) >> 6

	if encodingType == 3 { // Special integer encoding.
		switch first & 0x3F {
		case 0: // 8‑bit signed integer
			b, err := r.ReadByte()
			if err != nil {
				return "", err
			}
			return strconv.Itoa(int(int8(b))), nil
		case 1: // 16‑bit signed integer (little‑endian)
			var buf [2]byte
			if _, err := io.ReadFull(r, buf[:]); err != nil {
				return "", err
			}
			val := int16(binary.LittleEndian.Uint16(buf[:]))
			return strconv.Itoa(int(val)), nil
		case 2: // 32‑bit signed integer (little‑endian)
			var buf [4]byte
			if _, err := io.ReadFull(r, buf[:]); err != nil {
				return "", err
			}
			val := int32(binary.LittleEndian.Uint32(buf[:]))
			return strconv.Itoa(int(val)), nil
		default:
			return "", fmt.Errorf("unsupported special encoding")
		}
	}

	// Not a special encoding: put the byte back and read the length
	if err := r.UnreadByte(); err != nil {
		return "", err
	}
	length, err := readLength(r)
	if err != nil {
		return "", fmt.Errorf("error reading bulk string length: %w", err)
	}
	if length < 0 {
		return "", fmt.Errorf("negative string length %d", length)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", fmt.Errorf("error reading bulk string: %w", err)
	}
	return string(buf), nil
}