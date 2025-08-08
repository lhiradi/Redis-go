package handlers

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/internal/db"
)

func formatStreamEntries(entries []db.StreamEntry) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("*%d\r\n", len(entries)))

	for _, entry := range entries {
		// Array for each entry: [ID, [field1, value1, field2, value2]]
		builder.WriteString(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(entry.ID), entry.ID))
		builder.WriteString(fmt.Sprintf("*%d\r\n", len(entry.Fields)*2))

		for key, value := range entry.Fields {
			builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key))
			builder.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
		}
	}

	return builder.String()
}
