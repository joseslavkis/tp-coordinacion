package sum

import "hash/crc32"

func aggregationFor(clientID, fruit string, aggregationAmount int) int {
	return int(uint64(crc32.ChecksumIEEE([]byte(clientID+"\x00"+fruit))) % uint64(aggregationAmount))
}
