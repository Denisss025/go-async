package async

import (
	"context"

	"github.com/denisss025/go-async/pipeline"
)

var _ = context.Background()

type futureResult[T any] struct {
	Val T
	Err error
}

type Future[T any] struct {
	deferFn <-chan func()
	result  <-chan futureResult[T]
}

func (f *Future[T]) Await(ctx context.Context) (v T, err error) {
	defer func() {
		if f.deferFn != nil {
			fn := <-f.deferFn
			fn()
		}
	}()

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
	var executed bool

	return &Future[T]{
		result: pipeline.Generate(ctx,
			func(_ context.Context) (r futureResult[T], ok bool) {
				if ok = !executed; ok {
					r.Val, r.Err = fn(ctx)
				}

				return r, ok
			}),
	}
}

func ExecWithDefer[T any](ctx context.Context,
	fn func(context.Context) (func(), T, error)) *Future[T] {
	var executed bool

	dfn := make(chan func())

	return &Future[T]{
		deferFn: dfn,
		result: pipeline.Generate(ctx,
			func(ctx context.Context) (r futureResult[T], ok bool) {
				if ok = !executed; !ok {
					return r, ok
				}

				var deferFn func()

				deferFn, r.Val, r.Err = fn(ctx)

				go func(out chan<- func(), fn func()) {
					defer close(out)

					if deferFn != nil {
						out <- fn
					} else {
						out <- func() {}
					}
				}(dfn, deferFn)

				return r, ok
			}),
	}
}

func Then[T, V any](ctx context.Context, first *Future[T],
	next func(context.Context, T) (V, error)) *Future[V] {
	deferFn := first.deferFn
	first.deferFn = nil

	future := Exec(ctx, func(ctx context.Context) (v V, err error) {
		t, err := first.Await(ctx)
		if err == nil {
			v, err = next(ctx, t)
		}

		return v, err
	})

	if deferFn != nil {
		future.deferFn = deferFn
	}

	return future
}

func ThenWithDefer[T, V any](ctx context.Context, first *Future[T],
	next func(context.Context, T) (func(), V, error)) *Future[V] {
	deferFn := first.deferFn
	first.deferFn = nil

	future := ExecWithDefer(ctx,
		func(ctx context.Context) (dfn func(), v V, err error) {
			t, err := first.Await(ctx)
			if err == nil {
				dfn, v, err = next(ctx, t)
			}

			return func() {
				if dfn != nil {
					dfn()
				}

				if deferFn != nil {
					dfn = <-deferFn
					dfn()
				}
			}, v, err
		})

	if deferFn != nil {
		future.deferFn = deferFn
	}

	return future
}
