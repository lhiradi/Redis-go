package utils

import (
	"bufio"
	"fmt"
	"io"
	"log"
)

func parseArgs(reader *bufio.Reader) []string {
	line, err := reader.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return nil
		}
		log.Print(err)
		return nil
	}

	if len(line) < 2 || line[0] != '*' {
		log.Printf("Invalid RESP format: %s", line)
		return nil
	}

	var arrayLength int
	_, err = fmt.Sscanf(line, "*%d\r\n", &arrayLength)
	if err != nil {
		log.Printf("Failed to parse array length: %v", err)
		return nil
	}

	args := make([]string, arrayLength)
	for i := 0; i < arrayLength; i++ {
		lengthLine, err := reader.ReadString('\n')
		if err != nil {
			log.Print(err)
			return nil
		}

		if len(lengthLine) < 2 || lengthLine[0] != '$' {
			log.Printf("Invalid bulk string format: %s", lengthLine)
			return nil
		}

		var strLen int
		_, err = fmt.Sscanf(lengthLine, "$%d\r\n", &strLen)
		if err != nil {
			log.Printf("Failed to parse string length: %v", err)
			return nil
		}

		arg := make([]byte, strLen+2)
		_, err = io.ReadFull(reader, arg)
		if err != nil {
			log.Print(err)
			return nil
		}

		args[i] = string(arg[:strLen])
	}
	return args
}
