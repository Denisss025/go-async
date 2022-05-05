package pipeline

import "context"

// Generate sends to an output channel the results of function gen call.
// It closes the channel either when function gen returns false or when
// the context is done.
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

// Unroll takes a channel of slices and sends values of income slices
// to a new channel.
func Unroll[T any](ctx context.Context, in <-chan []T) <-chan T {
	c := make(chan T)

	go func(ctx context.Context, out chan<- T, in <-chan []T) {
		defer close(out)

		for arr := range in {
			for _, val := range arr {
				select {
				case <-ctx.Done():
					return
				case out <- val:
				}
			}
		}
	}(ctx, c, OrDone(ctx, in))

	return c
}

// ToChan creates a channel and sends all the values into it.
func ToChan[T any](ctx context.Context, values ...T) <-chan T {
	return SliceToChan(ctx, values)
}

// SliceToChan creates a channel and sends all the values of a given slice
// into it.
func SliceToChan[T any](ctx context.Context, slice []T) <-chan T {
	var i int

	return Generate(ctx, func(_ context.Context) (v T, ok bool) {
		if ok = i < len(slice); ok {
			v = slice[i]
		}

		i++

		return v, ok
	})
}

// Rangeable is an interface for all the signed numbers, i.e. ints and floats.
type Rangeable interface {
	~int8 | ~int16 | ~int32 | ~int64 | ~int | ~float32 | ~float64
}

// Range creates a channel that returns sequence of numbers with a given step.
// Panics when the step does not allow to reach the last value without overflow.
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
