package buckets

import (
	"fmt"
	"strconv"

	"git.tu-berlin.de/dergoegge/bachelor_thesis/csvlog"
)

type IntBuckets struct {
	log *csvlog.CsvLogger

	// bucket id -> bucket count
	buckets    []uint64
	bucketSize uint64
}

func NewIntBuckets(bucketSize uint64, logLocation string) *IntBuckets {
	return &IntBuckets{
		log:        csvlog.NewCsvLogger(logLocation, []string{"bucket_size", "bucket_id", "count"}),
		buckets:    make([]uint64, 0, 5000),
		bucketSize: bucketSize,
	}
}

func (b *IntBuckets) Put(num uint64) {
	bucketId := num / b.bucketSize
	if len(b.buckets)-1 < int(bucketId) {
		newBuckets := make([]uint64, int(bucketId)-(len(b.buckets)-1))
		if len(newBuckets) > 1000000 {
			fmt.Println(num)
			panic("too many buckets")
		}
		b.buckets = append(b.buckets, newBuckets...)
	}

	b.buckets[bucketId]++
}

func (b *IntBuckets) PutCount(num, count uint64) {
	bucketId := num / b.bucketSize
	if len(b.buckets)-1 < int(bucketId) {
		newBuckets := make([]uint64, int(bucketId)-(len(b.buckets)-1))
		b.buckets = append(b.buckets, newBuckets...)
	}

	b.buckets[bucketId] += count
}

func (b *IntBuckets) Flush() {
	for i := 0; i < len(b.buckets); i++ {
		b.log.MustLog([]string{
			strconv.Itoa(int(b.bucketSize)),
			strconv.Itoa(int(i)),
			strconv.Itoa(int(b.buckets[i])),
		})
	}
	b.log.Flush()
}
