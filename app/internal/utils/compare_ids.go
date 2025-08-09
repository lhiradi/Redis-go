package utils

func CompareIDs(ms1, seq1, ms2, seq2 int64) int {
	if ms1 != ms2 {
		if ms1 > ms2 {
			return 1
		}
		return -1
	}
	if seq1 != seq2 {
		if seq1 > seq2 {
			return 1
		}
		return -1
	}
	return 0
}
