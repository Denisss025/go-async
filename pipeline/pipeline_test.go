package pipeline

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testGen(ctx context.Context, values []int) <-chan int {
	c := make(chan int)

	go func(ctx context.Context, out chan<- int, v []int) {
		defer close(out)

		for _, i := range v {
			select {
			case <-ctx.Done():
				return
			case out <- i:
			}
		}
	}(ctx, c, values)

	return c
}

func testToSlice(wg *sync.WaitGroup, out *[]int, in <-chan int) {
	defer wg.Done()

	var result []int

	for v := range in {
		result = append(result, v)
	}

	*out = result
}

func testTeeHelper(t *testing.T, expect []int, chans []<-chan int) {
	t.Helper()

	r := make([][]int, len(chans))
	wg := &sync.WaitGroup{}
	wg.Add(len(r))

	for i := range chans {
		go testToSlice(wg, &r[i], chans[i])
	}

	wg.Wait()

	for _, result := range r {
		assert.Len(t, result, len(expect))
		assert.Equal(t, expect, result)
	}
}

func testTeeCancel(t *testing.T, num int, d1, d2 time.Duration) {
	t.Helper()

	var (
		pipe, c1, c2 <-chan int
		r1, r2       []int

		expect = make([]int, num)
	)

	for i := range expect {
		expect[i] = i - num/2
	}

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	pipe = ToChan(ctx, expect...)
	c1, c2 = Tee(ctx, pipe)

	wg := &sync.WaitGroup{}
	wg.Add(2)

	do := func(wg *sync.WaitGroup, out *[]int, in <-chan int,
		d time.Duration) {
		defer wg.Done()

		for v := range in {
			select {
			case <-time.After(d):
			}

			*out = append(*out, v)

			if v == 0 {
				cancel()
			}
		}
	}

	go do(wg, &r1, c1, d1)
	go do(wg, &r2, c2, d2)

	wg.Wait()

	assert.LessOrEqual(t, len(r1), num/2+1)
	assert.LessOrEqual(t, len(r2), num/2+1)
	assert.GreaterOrEqual(t, len(r1), num/2-1)
	assert.GreaterOrEqual(t, len(r2), num/2-1)
}

func TestAccumulate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mul := func(a, b int) (int, error) { return a * b, nil }
	numbers := make([]int, 6)
	expect := 1

	for i := range numbers {
		numbers[i] = i + 1
		expect *= i + 1
	}

	t.Run("factorial", func(t *testing.T) {
		t.Parallel()

		pipe := testGen(ctx, numbers)

		result, err := Accumulate(ctx, mul, 1, pipe)
		assert.NoError(t, err)
		assert.Equal(t, expect, result)
	})

	t.Run("stop on error", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(ctx)

		defer cancel()

		pipe := testGen(ctx, numbers)

		result, err := Accumulate(ctx,
			func(v int, t int) (int, error) {
				if v > 5 {
					return 0, io.ErrClosedPipe
				}

				return v * t, nil
			}, 1, pipe)

		assert.Error(t, err)
		assert.Zero(t, result)
	})

	t.Run("stop on cancel", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(ctx)

		defer cancel()

		pipe := testGen(ctx, numbers)

		result, err := Accumulate(ctx,
			func(v int, t int) (int, error) {
				if v > 5 {
					cancel()
				}

				return v * t, nil
			}, 1, pipe)

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ctx.Err()))
		assert.NotZero(t, result)
		assert.Greater(t, expect, result)
	})
}

func TestCollector(t *testing.T) {
	t.Parallel()

	const sliceLen = 5

	ctx := context.Background()
	sliceCollector := func(_ context.Context, in <-chan int) (
		r []int, ok bool) {
		r = make([]int, 0, sliceLen)

		for i := 0; i < sliceLen; i++ {
			v, ok := <-in
			if !ok {
				return r, len(r) > 0
			}

			r = append(r, v)
		}

		return r, len(r) > 0
	}

	t.Run("collect to slice", func(t *testing.T) {
		t.Parallel()

		pipe := Range(ctx, 0, 30)
		slicesPipe := Collector(ctx, sliceCollector, pipe)

		var first int

		for slice := range slicesPipe {
			assert.Len(t, slice, sliceLen)

			for i, v := range slice {
				assert.Equal(t, first+i, v)
			}

			first += sliceLen
		}
	})

	t.Run("collect stop", func(t *testing.T) {
		t.Parallel()

		var cancel context.CancelFunc

		ctx, cancel = context.WithCancel(ctx)

		defer cancel()

		pipe := Range(ctx, 0, 30)
		slicesPipe := Collector(ctx, sliceCollector, pipe)

		slice, ok := <-slicesPipe
		assert.True(t, ok)
		assert.Len(t, slice, sliceLen)

		cancel()

		slice, ok = <-slicesPipe
		if ok {
			assert.GreaterOrEqual(t, sliceLen, len(slice))
		}

		_, ok = <-slicesPipe
		assert.False(t, ok)
	})
}

func TestFilter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	nums := []int{1, 2, 3, 4, 5}
	evens := []int{2, 4}

	isEven := func(a int) bool { return a%2 == 0 }

	t.Run("accept all", func(t *testing.T) {
		t.Parallel()

		pipe := ToChan(ctx, nums...)
		filter := Filter(ctx, func(_ int) bool { return true }, pipe)

		var result []int

		for v := range filter {
			result = append(result, v)
		}

		assert.Len(t, result, len(nums))
		assert.Equal(t, result, nums)
	})

	t.Run("accept even", func(t *testing.T) {
		t.Parallel()

		pipe := ToChan(ctx, nums...)
		evenc := Filter(ctx, isEven, pipe)

		var result []int

		for v := range evenc {
			result = append(result, v)
		}

		assert.Len(t, result, 2)
		assert.Equal(t, evens, result)
	})
}

func TestTee(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	nums := make([]int, 100)

	for i := range nums {
		nums[i] = i - 49
	}

	t.Run("single tee", func(t *testing.T) {
		t.Parallel()

		pipe := ToChan(ctx, nums...)
		c1, c2 := Tee(ctx, pipe)

		assert.NotEqual(t, pipe, c1)
		assert.NotEqual(t, pipe, c2)
		assert.NotEqual(t, c1, c2)

		testTeeHelper(t, nums, []<-chan int{c1, c2})
	})

	t.Run("double tee", func(t *testing.T) {
		t.Parallel()

		pipe := ToChan(ctx, nums...)
		c1, tee := Tee(ctx, pipe)
		c2, c3 := Tee(ctx, tee)

		assert.NotEqual(t, pipe, c2)
		assert.NotEqual(t, pipe, c3)
		assert.NotEqual(t, c1, c2)
		assert.NotEqual(t, c1, c2)
		assert.NotEqual(t, tee, c1)
		assert.NotEqual(t, tee, c2)
		assert.NotEqual(t, tee, c3)

		testTeeHelper(t, nums, []<-chan int{c1, c2, c3})
	})

	t.Run("cancel", func(t *testing.T) {
		t.Parallel()

		testTeeCancel(t, len(nums), 0, time.Microsecond)
		testTeeCancel(t, len(nums), time.Microsecond, 0)
	})
}

func TestSplit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	nums := make([]int, 100)

	for i := range nums {
		nums[i] = i - 49
	}

	t.Run("split 1", func(t *testing.T) {
		t.Parallel()

		const n = 1

		pipe := ToChan(ctx, nums...)
		split := Split(ctx, pipe, n)
		assert.Len(t, split, n)
		assert.Equal(t, pipe, split[0])

		testTeeHelper(t, nums, split)
	})

	t.Run("split 2", func(t *testing.T) {
		t.Parallel()

		const n = 2

		pipe := ToChan(ctx, nums...)
		split := Split(ctx, pipe, n)
		assert.Len(t, split, n)
		assert.NotEqual(t, pipe, split[0])

		testTeeHelper(t, nums, split)
	})

	t.Run("split 5", func(t *testing.T) {
		t.Parallel()

		const n = 5

		pipe := ToChan(ctx, nums...)
		split := Split(ctx, pipe, n)
		assert.Len(t, split, n)

		testTeeHelper(t, nums, split)
	})
}

func TestSpread(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	nums := make([]int, 100)

	for i := range nums {
		nums[i] = i - len(nums)/2 + 1
	}

	t.Run("spread 1", func(t *testing.T) {
		t.Parallel()

		const n = 1

		pipe := ToChan(ctx, nums...)
		spread := Spread(ctx, pipe, n)
		assert.Len(t, spread, n)
		assert.Equal(t, pipe, spread[0])

		testTeeHelper(t, nums, spread)
	})

	t.Run("spread 5", func(t *testing.T) {
		t.Parallel()

		const n = 5

		pipe := ToChan(ctx, nums...)
		spread := Spread(ctx, pipe, n)
		assert.Len(t, spread, n)
		assert.NotEqual(t, pipe, spread[0])

		r := make([][]int, n)

		wg := &sync.WaitGroup{}
		wg.Add(n)

		for i := range spread {
			go testToSlice(wg, &r[i], spread[i])
		}

		wg.Wait()

		result := make([]int, 0, len(nums))

		for _, slice := range r {
			assert.Greater(t, len(slice), 0)
			assert.Less(t, len(slice), len(nums))

			result = append(result, slice...)
		}

		sort.Ints(result)

		assert.Equal(t, nums, result)
	})

}
