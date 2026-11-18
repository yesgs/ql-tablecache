package tablecache

import (
	"testing"
	"time"
)

func TestJitter(t *testing.T) {
	base := 100 * time.Millisecond

	// Test with 20% jitter
	for i := 0; i < 100; i++ {
		result := jitter(base, 20)
		min := 80 * time.Millisecond
		max := 120 * time.Millisecond
		if result < min || result > max {
			t.Errorf("jitter(%v, 20) = %v, want [%v, %v]", base, result, min, max)
		}
	}

	// Test with 0% jitter (should return base)
	result := jitter(base, 0)
	if result != base {
		t.Errorf("jitter(%v, 0) = %v, want %v", base, result, base)
	}

	// Test with negative percent (should return base)
	result = jitter(base, -10)
	if result != base {
		t.Errorf("jitter(%v, -10) = %v, want %v", base, result, base)
	}

	// Test with 100% or more (should return base)
	result = jitter(base, 100)
	if result != base {
		t.Errorf("jitter(%v, 100) = %v, want %v", base, result, base)
	}
}

func TestJitterDistribution(t *testing.T) {
	base := 1000 * time.Millisecond
	percent := 20
	samples := 1000

	var sum time.Duration
	for i := 0; i < samples; i++ {
		sum += jitter(base, percent)
	}

	// Average should be close to base
	avg := sum / time.Duration(samples)
	tolerance := base / 10 // 10% tolerance
	if avg < base-tolerance || avg > base+tolerance {
		t.Errorf("average jitter = %v, want ~%v (within %v)", avg, base, tolerance)
	}
}
