package pipeline

import (
	"context"
	"errors"
	"io"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

func TestPipeline(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(PipeTestSuite))
}

type PipeTestSuite struct {
	suite.Suite

	Ctx    context.Context
	Cancel context.CancelFunc
	Nums   []int
}

func (s *PipeTestSuite) SetupSuite() {
	s.Ctx, s.Cancel = context.WithCancel(context.Background())

	s.Nums = make([]int, 100)

	for i := range s.Nums {
		s.Nums[i] = i - len(s.Nums)/2
	}
}

func (s *PipeTestSuite) TearDownSuite() {
	s.Cancel()
}

func (s PipeTestSuite) IntPipe(ctx context.Context) <-chan int {
	return ToChan(ctx, s.Nums...)
}

func testToSlice(wg *sync.WaitGroup, out *[]int, in <-chan int) {
	defer wg.Done()

	var result []int

	for v := range in {
		result = append(result, v)
	}

	*out = result
}

func testTeeHelper(s *suite.Suite, expect []int, chans []<-chan int) {
	s.T().Helper()

	r := make([][]int, len(chans))
	wg := &sync.WaitGroup{}
	wg.Add(len(r))

	for i := range chans {
		go testToSlice(wg, &r[i], chans[i])
	}

	wg.Wait()

	for _, result := range r {
		s.Len(result, len(expect))
		s.Equal(expect, result)
	}
}

func testTeeCancel(s *suite.Suite, num int, d1, d2 time.Duration) {
	s.T().Helper()

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

	s.LessOrEqual(len(r1), num/2+2)
	s.LessOrEqual(len(r2), num/2+2)
	s.GreaterOrEqual(len(r1), num/2-2)
	s.GreaterOrEqual(len(r2), num/2-2)
}

func (s *PipeTestSuite) TestAccumulate() {
	sum := func(sum, val int) (int, error) {
		if val > 0 {
			return sum + val, nil
		}

		return sum - val, nil
	}

	abs := func(a int) int {
		if a > 0 {
			return a
		}

		return -a
	}

	var expected int

	for _, val := range s.Nums {
		expected += abs(val)
	}

	s.Run("sum", func() {
		pipe := s.IntPipe(s.Ctx)

		result, err := Accumulate(s.Ctx, sum, 0, pipe)
		s.NoError(err)
		s.Equal(expected, result)
	})

	s.Run("stop on error", func() {
		ctx, cancel := context.WithCancel(s.Ctx)

		defer cancel()

		pipe := s.IntPipe(ctx)

		result, err := Accumulate(ctx,
			func(v int, t int) (int, error) {
				if v > 5 {
					return 0, io.ErrClosedPipe
				}

				return v * t, nil
			}, 1, pipe)

		s.Error(err)
		s.Zero(result)
	})

	s.Run("stop on cancel", func() {
		ctx, cancel := context.WithCancel(s.Ctx)

		defer cancel()

		pipe := s.IntPipe(ctx)

		result, err := Accumulate(ctx,
			func(v int, t int) (int, error) {
				if v > 5 {
					cancel()
				}

				return v * t, nil
			}, 1, pipe)

		s.Error(err)
		s.True(errors.Is(err, ctx.Err()))
		s.NotZero(result)
		s.Greater(expected, result)
	})
}

func (s *PipeTestSuite) TestCollector() {
	const sliceLen = 5

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

	s.Run("collect to slice", func() {
		pipe := Range(s.Ctx, 0, 30)
		slicesPipe := Collector(s.Ctx, sliceCollector, pipe)

		var first int

		for slice := range slicesPipe {
			s.Len(slice, sliceLen)

			for i, v := range slice {
				s.Equal(first+i, v)
			}

			first += sliceLen
		}
	})

	s.Run("collect stop", func() {
		ctx, cancel := context.WithCancel(s.Ctx)

		defer cancel()

		pipe := Range(ctx, 0, 30)
		slicesPipe := Collector(ctx, sliceCollector, pipe)

		slice, ok := <-slicesPipe
		s.True(ok)
		s.Len(slice, sliceLen)

		cancel()

		slice, ok = <-slicesPipe
		if ok {
			s.GreaterOrEqual(sliceLen, len(slice))
		}

		_, ok = <-slicesPipe
		s.False(ok)
	})
}

func (s *PipeTestSuite) TestFilter() {
	evens := make([]int, 0, len(s.Nums)+1)
	isEven := func(a int) bool { return a%2 == 0 }

	for _, val := range s.Nums {
		if isEven(val) {
			evens = append(evens, val)
		}
	}

	s.Run("accept all", func() {
		pipe := s.IntPipe(s.Ctx)
		filter := Filter(s.Ctx, func(_ int) bool { return true }, pipe)

		var result []int

		for v := range filter {
			result = append(result, v)
		}

		s.Len(result, len(s.Nums))
		s.Equal(result, s.Nums)
	})

	s.Run("accept even", func() {
		pipe := s.IntPipe(s.Ctx)
		evenc := Filter(s.Ctx, isEven, pipe)

		var result []int

		for v := range evenc {
			result = append(result, v)
		}

		s.Len(result, len(evens))
		s.Equal(evens, result)
	})
}

func (s *PipeTestSuite) TestLimit() {
	s.Run("limit n = 0", func() {
		pipe := s.IntPipe(s.Ctx)
		limit := Limit(s.Ctx, 0, pipe)

		s.Nil(limit)
	})

	s.Run("limit n = len/2", func() {
		ctx, cancel := context.WithCancel(s.Ctx)

		defer cancel()

		pipe := s.IntPipe(ctx)
		limit := Limit(ctx, len(s.Nums)/2, pipe)

		result, err := Accumulate(ctx,
			func(v []int, t int) ([]int, error) {
				return append(v, t), nil
			}, nil, limit)

		s.NoError(err)
		s.Len(result, len(s.Nums)/2)
		s.Equal(s.Nums[:len(s.Nums)/2], result)
	})

	s.Run("limit n = len*2", func() {
		pipe := s.IntPipe(s.Ctx)
		limit := Limit(s.Ctx, len(s.Nums)*2, pipe)

		result, err := Accumulate(s.Ctx,
			func(v []int, t int) ([]int, error) {
				return append(v, t), nil
			}, nil, limit)

		s.NoError(err)
		s.Len(result, len(s.Nums))
		s.Equal(s.Nums, result)
	})

	s.Run("cancel", func() {
		ctx, cancel := context.WithCancel(s.Ctx)

		defer cancel()

		pipe := s.IntPipe(ctx)
		limit := Limit(ctx, len(s.Nums)/2, pipe)

		var result []int

		for val := range limit {
			result = append(result, val)

			if len(result) >= len(s.Nums)/4 {
				cancel()
			}
		}

		s.Len(result, len(s.Nums)/4)
		s.Equal(s.Nums[:len(s.Nums)/4], result)
	})
}

func (s *PipeTestSuite) TestMerge() {
	s.Run("zero and single channels", func() {
		merge := Merge[int](s.Ctx)
		s.Nil(merge)

		pipe := s.IntPipe(s.Ctx)
		merge = Merge(s.Ctx, pipe)

		s.Equal(pipe, merge)

		merge = Merge(s.Ctx, pipe, pipe, pipe)

		result, err := Accumulate(s.Ctx,
			func(v []int, t int) ([]int, error) {
				return append(v, t), nil
			}, nil, merge)

		s.NoError(err)
		s.Len(result, len(s.Nums))

		sort.Ints(result)
		s.Equal(s.Nums, result)
	})

	s.Run("multiple channels", func() {
		n := len(s.Nums) / 4

		pipe1 := ToChan(s.Ctx, s.Nums[:n]...)
		pipe2 := ToChan(s.Ctx, s.Nums[n:]...)

		pipe := Merge(s.Ctx, pipe1, pipe2)

		result, err := Accumulate(s.Ctx,
			func(v []int, t int) ([]int, error) {
				return append(v, t), nil
			}, nil, pipe)

		s.NoError(err)
		s.Len(result, len(s.Nums))
		s.NotEqual(s.Nums, result)

		sort.Ints(result)
		s.Equal(s.Nums, result)
	})
}

func (s *PipeTestSuite) TestTee() {
	s.Run("single tee", func() {
		pipe := s.IntPipe(s.Ctx)
		c1, c2 := Tee(s.Ctx, pipe)

		s.NotEqual(pipe, c1)
		s.NotEqual(pipe, c2)
		s.NotEqual(c1, c2)

		testTeeHelper(&s.Suite, s.Nums, []<-chan int{c1, c2})
	})

	s.Run("double tee", func() {
		pipe := s.IntPipe(s.Ctx)
		c1, tee := Tee(s.Ctx, pipe)
		c2, c3 := Tee(s.Ctx, tee)

		s.NotEqual(pipe, c2)
		s.NotEqual(pipe, c3)
		s.NotEqual(c1, c2)
		s.NotEqual(c1, c2)
		s.NotEqual(tee, c1)
		s.NotEqual(tee, c2)
		s.NotEqual(tee, c3)

		testTeeHelper(&s.Suite, s.Nums, []<-chan int{c1, c2, c3})
	})

	s.Run("cancel", func() {
		testTeeCancel(&s.Suite, len(s.Nums), 0, time.Microsecond)
		testTeeCancel(&s.Suite, len(s.Nums), time.Microsecond, 0)
	})
}

func (s *PipeTestSuite) TestSplit() {
	s.Run("split 1", func() {
		const n = 1

		pipe := s.IntPipe(s.Ctx)
		split := Split(s.Ctx, pipe, n)
		s.Len(split, n)
		s.Equal(pipe, split[0])

		testTeeHelper(&s.Suite, s.Nums, split)
	})

	s.Run("split 2", func() {
		const n = 2

		pipe := s.IntPipe(s.Ctx)
		split := Split(s.Ctx, pipe, n)
		s.Len(split, n)

		for i := range split {
			s.NotEqual(pipe, split[i])
		}

		testTeeHelper(&s.Suite, s.Nums, split)
	})

	s.Run("split 5", func() {
		const n = 5

		pipe := s.IntPipe(s.Ctx)
		split := Split(s.Ctx, pipe, n)
		s.Len(split, n)

		for i := range split {
			s.NotEqual(pipe, split[i])
		}

		testTeeHelper(&s.Suite, s.Nums, split)
	})
}

func (s *PipeTestSuite) TestSpread() {
	s.Run("spread 1", func() {
		const n = 1

		pipe := s.IntPipe(s.Ctx)
		spread := Spread(s.Ctx, pipe, n)
		s.Len(spread, n)
		s.Equal(pipe, spread[0])

		testTeeHelper(&s.Suite, s.Nums, spread)
	})

	s.Run("spread 5", func() {
		const n = 5

		pipe := s.IntPipe(s.Ctx)
		spread := Spread(s.Ctx, pipe, n)
		s.Len(spread, n)
		s.NotEqual(pipe, spread[0])

		r := make([][]int, n)

		wg := &sync.WaitGroup{}
		wg.Add(n)

		for i := range spread {
			go testToSlice(wg, &r[i], spread[i])
		}

		wg.Wait()

		result := make([]int, 0, len(s.Nums))

		for _, slice := range r {
			s.Greater(len(slice), 0)
			s.Less(len(slice), len(s.Nums))

			result = append(result, slice...)
		}

		sort.Ints(result)

		s.Equal(s.Nums, result)
	})
}
