package pipeline

import (
	"context"
	"sync"
)

// Map transforms an input chan to an output chan.
func Map[T, V any](ctx context.Context, mapFn func(T) V, input <-chan T) (
	output <-chan V) {
	c := make(chan V)

	go func(ctx context.Context, fn func(T) V, out chan<- V, in <-chan T) {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}

				select {
				case <-ctx.Done():
				case out <- fn(v):
				}
			}
		}
	}(ctx, mapFn, c, input)

	return c
}

// OrDone returns a channel that checks if the given Context is Done.
func OrDone[T any](ctx context.Context, in <-chan T) <-chan T {
	return Map(ctx, func(t T) T { return t }, in)
}

// Filter sends filtered values of an input chan to an output chan.
func Filter[T any](ctx context.Context, filter func(T) bool, input <-chan T) (
	output <-chan T) {
	collect := func(_ context.Context, in <-chan T) (empty T, ok bool) {
		for v := range in {
			if filter(v) {
				return v, true
			}
		}

		return empty, false
	}

	return Collector(ctx, collect, input)
}

// Limit limits the channel capacity.
func Limit[T any](ctx context.Context, n int, input <-chan T) <-chan T {
	if n == 0 {
		return nil
	}

	c := make(chan T)

	go func(ctx context.Context, n int, out chan<- T, in <-chan T) {
		defer close(out)

		var i int

		for v := range in {
			select {
			case <-ctx.Done():
				return
			case out <- v:
				i++
			}

			if i >= n {
				return
			}
		}
	}(ctx, n, c, OrDone(ctx, input))

	return c
}

// Accumulate accumulates data from a given channel.
func Accumulate[T, V any](ctx context.Context, fn func(V, T) (V, error),
	initVal V, input <-chan T) (out V, err error) {
	out = initVal

	for v := range OrDone(ctx, input) {
		if out, err = fn(out, v); err != nil {
			return out, err
		}
	}

	select {
	case <-ctx.Done():
		err = ctx.Err()
	default:
	}

	return out, err
}

// Collector accumulates data from a given channel to some intermediate
// structure, e.g. slice or structure, and returns it as a new channel.
func Collector[T, V any](ctx context.Context,
	collect func(context.Context, <-chan T) (V, bool), input <-chan T) (
	output <-chan V) {
	c := make(chan V)

	go func(ctx context.Context, out chan<- V,
		fn func(context.Context, <-chan T) (V, bool), in <-chan T) {
		defer close(out)

		for {
			v, ok := fn(ctx, in)
			if !ok {
				return
			}

			select {
			case <-ctx.Done():
				return
			case out <- v:
			}
		}
	}(ctx, c, collect, OrDone(ctx, input))

	return c
}

// Merge creates a new input channel that returns the content of all the given
// channels.
func Merge[T any](ctx context.Context, chans ...<-chan T) <-chan T {
	switch len(chans) {
	case 0:
		return nil
	case 1:
		return chans[0]
	}

	c := make(chan T)

	wg := new(sync.WaitGroup)
	wg.Add(len(chans))

	fanout := func(wg *sync.WaitGroup, in <-chan T, out chan<- T) {
		defer wg.Done()

		for v := range in {
			out <- v
		}
	}

	for i := range chans {
		go fanout(wg, OrDone(ctx, chans[i]), c)
	}

	go func(wg *sync.WaitGroup, ch chan<- T) {
		defer close(ch)

		wg.Wait()
	}(wg, c)

	return c
}

// Spread returns n channels. Every channel partially repeats an input channel.
func Spread[T any](ctx context.Context, in <-chan T, num int) (out []<-chan T) {
	if num == 1 {
		return []<-chan T{in}
	}

	out = make([]<-chan T, num)

	for i := range out {
		out[i] = OrDone(ctx, in)
	}

	return out
}

// Split returns n channels that repeat an input channel.
func Split[T any](ctx context.Context, input <-chan T, n int) (out []<-chan T) {
	const teeNum = 2

	if n < teeNum {
		return []<-chan T{input}
	}

	out = make([]<-chan T, n)

	tee := input

	for i := range out[1:] {
		out[i+1], tee = Tee(ctx, tee)
	}

	out[0] = tee

	return
}

// Tee creates two channels that repeat an input channel.
func Tee[T any](ctx context.Context, input <-chan T) (out1, out2 <-chan T) {
	c1 := make(chan T)
	c2 := make(chan T)

	go func(ctx context.Context, out1 chan<- T, out2 chan<- T,
		in <-chan T) {
		defer close(out1)
		defer close(out2)

		for v := range OrDone(ctx, in) {
			out1, out2 := out1, out2

			select {
			case <-ctx.Done():
			case out1 <- v:
				out1 = nil
			case out2 <- v:
				out2 = nil
			}

			select {
			case <-ctx.Done():
			case out1 <- v:
			case out2 <- v:
			}
		}
	}(ctx, c1, c2, input)

	return c1, c2
}
