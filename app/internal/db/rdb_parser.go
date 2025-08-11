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

	// Check if the file exists. If not, return an empty map and no error.
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		fmt.Printf("RDB file not found at %s. Starting with an empty database.\n", filePath)
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

	magicNumber := make([]byte, 5)
	if _, err := io.ReadFull(reader, magicNumber); err != nil || string(magicNumber) != "REDIS" {
		return nil, fmt.Errorf("invalid RDB file magic number")
	}

	version := make([]byte, 4)
	if _, err := io.ReadFull(reader, version); err != nil {
		return nil, fmt.Errorf("invalid RDB file version")
	}

	data := make(map[string]cacheValue)

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
			if _, err := reader.ReadByte(); err != nil { // We don't care about the DB number
				return nil, fmt.Errorf("error reading database number: %w", err)
			}
		case 0xFD: // Expiry in seconds
			expiryTimeBytes := make([]byte, 4)
			if _, err := io.ReadFull(reader, expiryTimeBytes); err != nil {
				return nil, fmt.Errorf("error reading expiry time in seconds: %w", err)
			}

		case 0xFC: // Expiry in milliseconds
			expiryTimeBytes := make([]byte, 8)
			if _, err := io.ReadFull(reader, expiryTimeBytes); err != nil {
				return nil, fmt.Errorf("error reading expiry time in milliseconds: %w", err)
			}
		case 0x00: // A string value type
			key, err := readString(reader)
			if err != nil {
				return nil, fmt.Errorf("error reading key: %w", err)
			}
			value, err := readString(reader)
			if err != nil {
				return nil, fmt.Errorf("error reading value: %w", err)
			}
			// Store the key and value with a default TTL
			data[key] = cacheValue{Value: value, Ttl: 0}
		case 0xFF: // End of file
			return data, nil
		}
	}
	return data, nil
}

func readLength(reader *bufio.Reader) (int, error) {
	firstByte, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}

	switch (firstByte & 0xC0) >> 6 {
	case 0:
		return int(firstByte), nil
	case 1:
		nextByte, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		return int(firstByte&0x3F)<<8 | int(nextByte), nil
	case 2:
		lengthBytes := make([]byte, 4)
		if _, err := io.ReadFull(reader, lengthBytes); err != nil {
			return 0, err
		}
		return int(binary.BigEndian.Uint32(lengthBytes)), nil
	default:
		// Compressed string. For this stage we assume simple string values.
		return 0, fmt.Errorf("unsupported length encoding type: %x", (firstByte&0xC0)>>6)
	}
}

// readString reads a length-prefixed string from the reader.
func readString(reader *bufio.Reader) (string, error) {
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
