package utils

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func ValidateStreamID(ID, lastID string) (string, error) {
	switch ID {
	case "*":
		return fmt.Sprintf("%d-%d", time.Now().UnixMilli(), 0), nil
	case "0-0":
		return "", fmt.Errorf(" The ID specified in XADD must be greater than 0-0")
	}

	if lastID == "" {
		IDParts := strings.Split(ID, "-")
		if len(IDParts) == 2 && IDParts[1] == "*" {

			if IDParts[0] == "0" {
				return "0-1", nil
			}
			return fmt.Sprintf("%s-0", IDParts[0]), nil
		}
		return ID, nil
	}

	lastParts := strings.Split(lastID, "-")
	lastMs, err := strconv.ParseInt(lastParts[0], 10, 64)
	if err != nil {
		return "", err
	}

	intLastSeq, err := strconv.ParseInt(lastParts[1], 10, 64)
	if err != nil {
		return "", err
	}

	IDParts := strings.Split(ID, "-")
	IDMs, err := strconv.ParseInt(IDParts[0], 10, 64)
	if err != nil {
		return "", err
	}

	IDSeq := IDParts[1]
	if IDSeq == "*" && IDMs == lastMs {
		return fmt.Sprintf("%d-%d", IDMs, intLastSeq+1), nil
	} else if IDSeq == "*" {
		return fmt.Sprintf("%d-%d", IDMs, 0), nil
	} else if IDSeq == "*" && IDMs == 0 {
		return fmt.Sprintf("%d-%d", IDMs, 1), nil
	}

	intIDSeq, err := strconv.ParseInt(IDSeq, 10, 64)
	if err != nil {
		return "", err
	}

	if IDMs < lastMs || (IDMs == lastMs && intIDSeq <= intLastSeq) {
		return "", fmt.Errorf(" The ID specified in XADD is equal or smaller than the target stream top item")
	}

	return ID, nil
}
