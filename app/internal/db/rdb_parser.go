package db

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
			key, err := readLengthPrefixedString(reader)
			if err != nil {
				return nil, err
			}
			value, err := readLengthPrefixedString(reader)
			if err != nil {
				return nil, err
			}
			data[key] = cacheValue{Value: value, Ttl: 0}

		case 0xFE: // SELECT DB — skip
			if _, err := readLength(reader); err != nil {
				return nil, err
			}
		case 0xFA: // AUX field
			if _, err := readLengthPrefixedString(reader); err != nil {
				return nil, fmt.Errorf("error reading AUX key: %w", err)
			}
			if _, err := readLengthPrefixedString(reader); err != nil {
				return nil, fmt.Errorf("error reading AUX value: %w", err)
			}
		case 0xFF: // End
			return data, nil
		default:
			return nil, fmt.Errorf("unsupported opcode: 0x%x", opcode)
		}
	}
	return data, nil
}

func readLengthPrefixedString(r *bufio.Reader) (string, error) {
	length, err := readLength(r)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readLength(r *bufio.Reader) (int, error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	switch (first & 0xC0) >> 6 {
	case 0: // 6-bit length
		return int(first & 0x3F), nil
	case 1: // 14-bit length
		second, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		return ((int(first) & 0x3F) << 8) | int(second), nil
	case 2: // 32-bit length
		var buf [4]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(buf[:])), nil
	default:
		return 0, fmt.Errorf("invalid length encoding")
	}
}
