package pipeline_test

import (
	"context"
	"testing"

	. "github.com/denisss025/go-async/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestRange(t *testing.T) {
	t.Parallel()

	const (
		from = 10
		to   = 99
		step = 5
	)

	t.Run("range without step", func(t *testing.T) {
		t.Parallel()

		ch := Range(context.Background(), from, to)
		i := from

		for v := range ch {
			assert.Equal(t, i, v)

			i++
		}

		assert.Equal(t, to, i)
	})

	t.Run("range with step", func(t *testing.T) {
		t.Parallel()

		ch := Range(context.Background(), from, to, step)
		i := from

		for v := range ch {
			assert.Equal(t, i, v)

			i += step
		}

		assert.GreaterOrEqual(t, i, to)
		assert.LessOrEqual(t, i, (to-from)/step*(step+1)+from)
	})

	t.Run("range backwards without step", func(t *testing.T) {
		t.Parallel()

		woStep := Range(context.Background(), to, from)
		i := to

		for v := range woStep {
			assert.Equal(t, i, v)

			i--
		}

		assert.Equal(t, from, i)
	})

	t.Run("range backwards with step", func(t *testing.T) {
		t.Parallel()

		woStep := Range(context.Background(), to, from, -step)
		i := to

		for v := range woStep {
			assert.Equal(t, i, v)

			i -= step
		}

		assert.LessOrEqual(t, i, from)
		assert.GreaterOrEqual(t, i, from-(to-from)/step*step)
	})

	t.Run("call cancel", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		woStep := Range(ctx, from, to)
		i := from

		for v := range woStep {
			if i > from+step {
				cancel()
			}

			assert.Equal(t, i, v)

			i++
		}

		cancel()

		assert.Greater(t, i, from+step)
		assert.Less(t, i, to)
	})

	t.Run("single value", func(t *testing.T) {
		t.Parallel()

		woStep := Range(context.Background(), from, from)

		v, ok := <-woStep
		assert.True(t, ok)
		assert.Equal(t, from, v)

		_, ok = <-woStep
		assert.False(t, ok)
	})

	t.Run("panic on wrong step", func(t *testing.T) {
		t.Parallel()

		assert.Panics(t, func() {
			_ = Range(context.Background(), from, to, -step)
		})

		assert.Panics(t, func() {
			_ = Range(context.Background(), from, -to, step)
		})
	})
}

func TestUnroll(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	newGen := func(ctx context.Context) <-chan []int {
		return Collector(context.Background(),
			func(ctx context.Context, in <-chan int) (
				arr []int, ok bool) {
				for v := range in {
					arr = append(arr, v)

					if len(arr) == 3 {
						return arr, true
					}
				}

				return arr, len(arr) > 0
			}, Range(ctx, 1, 100))
	}

	t.Run("without cancellation", func(t *testing.T) {
		t.Parallel()

		g := Unroll(ctx, newGen(ctx))
		arr := make([]int, 0, 99)

		for v := range g {
			arr = append(arr, v)
		}

		assert.Len(t, arr, 99)

		for i, v := range arr {
			assert.Equal(t, i+1, v)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		t.Parallel()

		ctx1, cancel1 := context.WithCancel(ctx)

		defer cancel1()

		ctx2, cancel := context.WithCancel(ctx1)

		defer cancel()

		g := Unroll(ctx2, newGen(ctx1))
		arr := make([]int, 0, 99)

		for v := range g {
			if arr = append(arr, v); len(arr) == 50 {
				cancel()
			}
		}

		assert.GreaterOrEqual(t, len(arr), 50)
		assert.LessOrEqual(t, len(arr), 51)

		for i, v := range arr {
			assert.Equal(t, i+1, v)
		}
	})
}
