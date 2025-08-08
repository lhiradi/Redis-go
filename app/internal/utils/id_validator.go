package utils

import (
	"fmt"
	"strconv"
	"strings"
)

func ValidateStreamID(ID, lastID string) (string, error) {
	finalID, err := GenerateStreamID(ID, lastID)
	if err != nil {
		return "", err
	}

	if finalID == "0-0" {
		return "", fmt.Errorf(" The ID specified in XADD must be greater than 0-0")
	}

	lastParts := strings.Split(lastID, "-")
	lastMs, _ := strconv.ParseInt(lastParts[0], 10, 64)
	intLastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)

	IDParts := strings.Split(finalID, "-")
	IDMs, _ := strconv.ParseInt(IDParts[0], 10, 64)
	intIDSeq, _ := strconv.ParseInt(IDParts[1], 10, 64)

	if IDMs < lastMs || (IDMs == lastMs && intIDSeq <= intLastSeq) {
		return "", fmt.Errorf(" The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return finalID, nil
}
