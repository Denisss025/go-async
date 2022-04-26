package async_test

import (
	"context"
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
