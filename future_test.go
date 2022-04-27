package async_test

import (
	"context"
	"io"
	"testing"
	"time"

	. "github.com/denisss025/go-async"
	"github.com/stretchr/testify/assert"
)

func TestExec(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("with delay", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(ctx, time.Second)

		defer cancel()

		getFive := Exec(ctx, func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(time.Microsecond):
				return 5, nil
			}
		})

		five, err := getFive.Await(ctx)

		assert.NoError(t, err)
		assert.Equal(t, 5, five)
	})

	t.Run("with timeout", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(ctx, time.Microsecond)

		defer cancel()

		getFive := Exec(ctx, func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 5, ctx.Err()
			case <-time.After(time.Second):
				return 5, nil
			}
		})

		zero, err := getFive.Await(ctx)

		assert.Error(t, err)
		assert.Zero(t, zero)
	})
}

func TestThen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("without error", func(t *testing.T) {
		t.Parallel()

		getFive := Exec(ctx, func(_ context.Context) (int, error) {
			return 5, nil
		})

		makeSlice := Then(ctx, getFive,
			func(_ context.Context, n int) ([]struct{}, error) {
				return make([]struct{}, n), nil
			})

		slice, err := makeSlice.Await(ctx)
		assert.NoError(t, err)
		assert.Len(t, slice, 5)
	})

	t.Run("Future.Then", func(t *testing.T) {
		t.Parallel()

		ten, err := Exec(ctx, func(_ context.Context) (int, error) {
			return 5, nil
		}).Then(ctx, func(_ context.Context, i int) (int, error) {
			return i * 2, nil
		}).Await(ctx)

		assert.NoError(t, err)
		assert.Equal(t, 10, ten)
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		var called bool

		zero, err := Exec(ctx, func(_ context.Context) (int, error) {
			return 5, io.EOF
		}).Then(ctx, func(_ context.Context, i int) (int, error) {
			called = true

			return i * 2, nil
		}).Await(ctx)

		assert.Error(t, err)
		assert.Zero(t, zero)
		assert.False(t, called)
	})
}
