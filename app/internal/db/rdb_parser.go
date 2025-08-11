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

	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		fmt.Printf("RDB file not found at %s. Starting with empty database.\n", filePath)
		return make(map[string]cacheValue), nil
	}
	if err != nil {
		return nil, fmt.Errorf("error stating RDB file: %w", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening RDB file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Check magic
	magic := make([]byte, 5)
	if _, err := io.ReadFull(reader, magic); err != nil || string(magic) != "REDIS" {
		return nil, fmt.Errorf("invalid RDB magic number")
	}

	version := make([]byte, 4)
	if _, err := io.ReadFull(reader, version); err != nil {
		return nil, fmt.Errorf("invalid RDB version")
	}

	data := make(map[string]cacheValue)
	var ttl int64 = 0 // TTL in ms for the next key

	for {
		opcode, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error reading opcode: %w", err)
		}

		switch opcode {
		case 0xFE: // DB selector
			_, err := readLength(reader) // DB number (length-encoded)
			if err != nil {
				return nil, fmt.Errorf("error reading DB selector: %w", err)
			}
		case 0xFA: // AUX key-value pair
			if _, err := readString(reader); err != nil {
				return nil, fmt.Errorf("error reading AUX key: %w", err)
			}
			if _, err := readString(reader); err != nil {
				return nil, fmt.Errorf("error reading AUX value: %w", err)
			}
		case 0xFD: // Expiry in seconds
			buf := make([]byte, 4)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return nil, fmt.Errorf("error reading expiry seconds: %w", err)
			}
			ttl = int64(binary.LittleEndian.Uint32(buf)) * 1000
		case 0xFC: // Expiry in milliseconds
			buf := make([]byte, 8)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return nil, fmt.Errorf("error reading expiry ms: %w", err)
			}
			ttl = int64(binary.LittleEndian.Uint64(buf))
		case 0x00: // String value type
			key, err := readString(reader)
			if err != nil {
				return nil, fmt.Errorf("error reading key: %w", err)
			}
			value, err := readString(reader)
			if err != nil {
				return nil, fmt.Errorf("error reading value: %w", err)
			}
			data[key] = cacheValue{Value: value, Ttl: ttl}
			ttl = 0 // Reset TTL after use
		case 0xFF: // End of RDB
			return data, nil
		}
	}

	return data, nil
}

// readLength decodes a length-encoded integer from RDB format
func readLength(reader *bufio.Reader) (int, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

	switch (firstByte & 0xC0) >> 6 {
	case 0: // 6-bit length
		return int(firstByte & 0x3F), nil
	case 1: // 14-bit length
		secondByte, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return ((int(firstByte) & 0x3F) << 8) | int(secondByte), nil
	case 2: // 32-bit length
		buf := make([]byte, 4)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(buf)), nil
	case 3: // special encoding
		return int(firstByte & 0x3F), nil
	default:
		return 0, fmt.Errorf("invalid length encoding")
	}
}

// readString reads a Redis-encoded string (possibly integer or LZF compressed)
func readString(reader *bufio.Reader) (string, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return "", err
	}

	// Check for special encoding
	// The top two bits being 11 indicate a special encoding, the next 6 bits are the encoding type
	encodingType := (firstByte & 0xC0) >> 6
	if encodingType == 3 {
		switch firstByte & 0x3F {
		case 0: // 8-bit int
			b, err := reader.ReadByte()
			if err != nil {
				return "", err
			}
			return strconv.Itoa(int(int8(b))), nil
		case 1: // 16-bit int
			buf := make([]byte, 2)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return "", err
			}
			return strconv.Itoa(int(int16(binary.LittleEndian.Uint16(buf)))), nil
		case 2: // 32-bit int
			buf := make([]byte, 4)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return "", err
			}
			return strconv.Itoa(int(int32(binary.LittleEndian.Uint32(buf)))), nil
		default:
			return "", fmt.Errorf("unsupported special encoding")
		}
	}

	// Normal raw string
	// The firstByte is a normal length header, we need to unread it and call readLength
	reader.UnreadByte()
	length, err := readLength(reader)
	if err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}
