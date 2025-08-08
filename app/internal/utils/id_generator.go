package utils

import (
	"fmt"
	"strconv"
	"strings"
)

func GenerateStreamID(ID, lastID string) (string, error) {
	if ID == "" {
		return "", fmt.Errorf("ID cannot be empty")
	}

	IDParts := strings.Split(ID, "-")
	if len(IDParts) != 2 {
		return ID, nil
	}
	
	if IDParts[1] == "*" {
		IDMs, err := strconv.ParseInt(IDParts[0], 10, 64)
		if err != nil {
			return "", err
		}

		if lastID == "" {
			if IDMs == 0 {
				return "0-1", nil
			}
			return fmt.Sprintf("%s-0", IDParts[0]), nil
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
	
		if IDMs < lastMs {
			return "", fmt.Errorf(" The ID specified in XADD is smaller than the target stream top item")
		} else if IDMs == lastMs {
			return fmt.Sprintf("%d-%d", IDMs, intLastSeq+1), nil
		}
	
		if IDMs == 0 {
			return "0-1", nil
		}
	
		return fmt.Sprintf("%d-0", IDMs), nil
	}

	return ID, nil
}