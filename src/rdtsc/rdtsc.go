package rdtsc

import "time"

var clockStart = time.Now()

// Measure elapsed time in nanoseconds using Go's monotonic clock
func Cputicks() uint64 {
	return uint64(time.Since(clockStart).Nanoseconds())
}
