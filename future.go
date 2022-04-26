package async

import (
	"context"

	"github.com/denisss025/go-async/pipeline"
)

type futureResult[T any] struct {
	Val T
	Err error
}

type Future[T any] struct {
	result <-chan futureResult[T]
}

func (f *Future[T]) Await(ctx context.Context) (v T, err error) {
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case result := <-f.result:
		v, err = result.Val, result.Err
	}

	return v, err
}

func Exec[T any](ctx context.Context, fn func(context.Context) (T, error)) (
	future *Future[T]) {
	return &Future[T]{
		result: pipeline.Map(ctx,
			func(ctx context.Context) (v futureResult[T]) {
				v.Val, v.Err = fn(ctx)

				return v
			}, pipeline.ToChan(ctx, ctx)),
	}
}

func Then[T, V any](ctx context.Context, first *Future[T],
	next func(context.Context, T) (V, error)) *Future[V] {
	return &Future[V]{
		result: pipeline.Map(ctx,
			func(t futureResult[T]) (v futureResult[V]) {
				if v.Err = t.Err; v.Err == nil {
					v.Val, v.Err = next(ctx, t.Val)
				}

				return v
			}, first.result),
	}
}
