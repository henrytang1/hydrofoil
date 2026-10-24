package rdtsc

import (
	"testing"
	"time"
)

func TestCputicks(t *testing.T) {
	start := Cputicks()
	time.Sleep(time.Millisecond)
	previous := Cputicks()
	if previous <= start {
		t.Fatal("clock did not advance")
	}
	for i := 0; i < 1000; i++ {
		current := Cputicks()
		if current < previous {
			t.Fatal("clock moved backward")
		}
		previous = current
	}
}
