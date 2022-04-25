package pipeline

import "context"

func Generate[T any](ctx context.Context, gen func(context.Context) (T, bool)) (
	output <-chan T) {
	c := make(chan T)

	go func(ctx context.Context, out chan<- T,
		next func(context.Context) (T, bool)) {
		defer close(c)

		for {
			v, ok := next(ctx)
			if !ok {
				return
			}

			select {
			case <-ctx.Done():
				return
			case out <- v:
			}
		}
	}(ctx, c, gen)

	return c
}

func ToChan[T any](ctx context.Context, values ...T) <-chan T {
	var i int

	return Generate(ctx, func(_ context.Context) (v T, ok bool) {
		if ok = i < len(values); ok {
			v = values[i]
		}

		i++

		return v, ok
	})
}

type Rangeable interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~int | ~float32 | ~float64
}

func Range[T Rangeable](ctx context.Context, from, to T, optStep ...T) (
	output <-chan T) {
	var step T = 1

	if from > to {
		step = -step
	}

	if len(optStep) > 0 {
		step = optStep[0]
	}

	// var gen func(context.Context, chan<- T, T, T, T)
	var next func(context.Context) (T, bool)

	switch {
	case from < to:
		if step <= 0 {
			panic("step must be greater than 0")
		}

		next = func(_ context.Context) (T, bool) {
			i := from
			from += step

			return i, i < to
		}
	case from > to:
		if step >= 0 {
			panic("step must be less than 0")
		}

		next = func(_ context.Context) (T, bool) {
			i := from
			from += step

			return i, i > to
		}
	default:
		return ToChan(ctx, from)
	}

	c := Generate(ctx, next)

	return c
}
