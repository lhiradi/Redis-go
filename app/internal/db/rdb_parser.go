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
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening RDB file: %w", err)
	}
	defer f.Close()

	reader := bufio.NewReader(f)

	// Magic string
	magic := make([]byte, 5)
	if _, err := io.ReadFull(reader, magic); err != nil || string(magic) != "REDIS" {
		return nil, fmt.Errorf("invalid RDB file")
	}

	// Version (ignore for now)
	version := make([]byte, 4)
	if _, err := io.ReadFull(reader, version); err != nil {
		return nil, fmt.Errorf("error reading RDB version: %w", err)
	}

	data := make(map[string]cacheValue)

	for {
		opcode, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		switch opcode {
		case 0x00: // String value
			key, err := readString(reader)
			if err != nil {
				return nil, err
			}
			value, err := readString(reader)
			if err != nil {
				return nil, err
			}
			data[key] = cacheValue{Value: value, Ttl: 0}

		case 0xFE: // SELECT DB — skip
			if _, err := readLength(reader); err != nil {
				return nil, err
			}
		case 0xFA: // AUX field
			if _, err := readString(reader); err != nil {
				return nil, fmt.Errorf("error reading AUX key: %w", err)
			}
			if _, err := readString(reader); err != nil {
				return nil, fmt.Errorf("error reading AUX value: %w", err)
			}
		case 0xFB:
			if _, err := readLength(reader); err != nil {
				return nil, err
			}
			if _, err := readLength(reader); err != nil {
				return nil, err
			}
		case 0xFF: // End
			return data, nil
		default:
			return nil, fmt.Errorf("unsupported opcode: 0x%x", opcode)
		}
	}
	return data, nil
}

func readString(r *bufio.Reader) (string, error) {
	length, isEncoded, err := readLengthAndEncoding(r)
	if err != nil {
		return "", err
	}
	if isEncoded {
		switch length {
		case 0, 1, 2:
			// Integer encoded as string
			var val int64
			switch length {
			case 0: // 8-bit integer
				var buf [1]byte
				if _, err := io.ReadFull(r, buf[:]); err != nil {
					return "", err
				}
				val = int64(buf[0])
			case 1: // 16-bit integer
				var buf [2]byte
				if _, err := io.ReadFull(r, buf[:]); err != nil {
					return "", err
				}
				val = int64(binary.LittleEndian.Uint16(buf[:]))
			case 2: // 32-bit integer
				var buf [4]byte
				if _, err := io.ReadFull(r, buf[:]); err != nil {
					return "", err
				}
				val = int64(binary.LittleEndian.Uint32(buf[:]))
			}
			return strconv.FormatInt(val, 10), nil
		case 3:
			// LZF compressed string. This is not supported in this stage.
			return "", fmt.Errorf("unsupported LZF compressed string")
		default:
			return "", fmt.Errorf("unsupported special encoding: %d", length)
		}
	}
	// Normal length-prefixed string
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readLengthAndEncoding(r *bufio.Reader) (int, bool, error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, false, err
	}
	switch (first & 0xC0) >> 6 {
	case 0: // 6-bit length
		return int(first & 0x3F), false, nil
	case 1: // 14-bit length
		second, err := r.ReadByte()
		if err != nil {
			return 0, false, err
		}
		return ((int(first) & 0x3F) << 8) | int(second), false, nil
	case 2: // 32-bit length
		var buf [4]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, false, err
		}
		return int(binary.BigEndian.Uint32(buf[:])), false, nil
	case 3: // Special encoding
		return int(first & 0x3F), true, nil
	default:
		return 0, false, fmt.Errorf("invalid length encoding")
	}
}

// readLength is only used for opcodes that are followed by a length
func readLength(r *bufio.Reader) (int, error) {
	length, isEncoded, err := readLengthAndEncoding(r)
	if err != nil {
		return 0, err
	}
	if isEncoded {
		return 0, fmt.Errorf("expected a length, but found an encoded string")
	}
	return length, nil
}
