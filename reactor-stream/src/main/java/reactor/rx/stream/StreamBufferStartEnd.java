package reactor.rx.stream;

import java.util.*;
import java.util.concurrent.atomic.*;
import reactor.fn.*;

import org.reactivestreams.*;

import reactor.core.error.Exceptions;
import reactor.core.subscription.DeferredSubscription;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.*;

/**
 * buffers elements into possibly overlapping buffers whose boundaries are determined
  by a start Publisher's element and a signal of a derived Publisher
 *
 * @param <T> the source value type
 * @param <U> the value type of the publisher opening the buffers
 * @param <V> the value type of the publisher closing the individual buffers
 * @param <C> the collection type that holds the buffered values
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamBufferStartEnd<T, U, V, C extends Collection<? super T>>
extends StreamBarrier<T, C> {

	final Publisher<U> start;
	
	final Function<? super U, ? extends Publisher<V>> end;
	
	final Supplier<C> bufferSupplier;
	
	final Supplier<? extends Queue<C>> queueSupplier;

	public StreamBufferStartEnd(Publisher<? extends T> source, Publisher<U> start,
			Function<? super U, ? extends Publisher<V>> end, Supplier<C> bufferSupplier,
					Supplier<? extends Queue<C>> queueSupplier) {
		super(source);
		this.start = Objects.requireNonNull(start, "start");
		this.end = Objects.requireNonNull(end, "end");
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
	}
	
	@Override
	public void subscribe(Subscriber<? super C> s) {
		
		Queue<C> q;
		
		try {
			q = queueSupplier.get();
		} catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}
		
		if (q == null) {
			EmptySubscription.error(s, new NullPointerException("The queueSupplier returned a null queue"));
			return;
		}
		
		StreamBufferStartEndMain<T, U, V, C> parent = new StreamBufferStartEndMain<>(s, bufferSupplier, q, end);
		
		s.onSubscribe(parent);
		
		start.subscribe(parent.starter);
		
		source.subscribe(parent);
	}
	
	static final class StreamBufferStartEndMain<T, U, V, C extends Collection<? super T>>
	implements Subscriber<T>, Subscription {
		
		final Subscriber<? super C> actual;
		
		final Supplier<C> bufferSupplier;
		
		final Queue<C> queue;
		
		final Function<? super U, ? extends Publisher<V>> end;
		
		Set<Subscription> endSubscriptions;
		
		final StreamBufferStartEndStarter<U> starter;

		Map<Long, C> buffers;
		
		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StreamBufferStartEndMain, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(StreamBufferStartEndMain.class, Subscription.class, "s");
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<StreamBufferStartEndMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(StreamBufferStartEndMain.class, "requested");

		long index;
		
		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamBufferStartEndMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(StreamBufferStartEndMain.class, "wip");
		
		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StreamBufferStartEndMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(StreamBufferStartEndMain.class, Throwable.class, "error");
		
		volatile boolean done;
		
		volatile boolean cancelled;

		volatile int open;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamBufferStartEndMain> OPEN =
				AtomicIntegerFieldUpdater.newUpdater(StreamBufferStartEndMain.class, "open");

		public StreamBufferStartEndMain(Subscriber<? super C> actual, Supplier<C> bufferSupplier, Queue<C> queue, Function<? super U, ? extends Publisher<V>> end) {
			this.actual = actual;
			this.bufferSupplier = bufferSupplier;
			this.buffers = new HashMap<>();
			this.endSubscriptions = new HashSet<>();
			this.queue = queue;
			this.end = end;
			this.open = 1;
			this.starter = new StreamBufferStartEndStarter<>(this);
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}
		
		@Override
		public void onNext(T t) {
			synchronized (this) {
				Map<Long, C> set = buffers;
				if (set != null) {
					for (C b : set.values()) {
						b.add(t);
					}
					return;
				}
			}
			
			Exceptions.onNextDropped(t);
		}
		
		@Override
		public void onError(Throwable t) {
			boolean report;
			synchronized (this) {
				Map<Long, C> set = buffers;
				if (set != null) {
					buffers = null;
					report = true;
				} else {
					report = false;
				}
			}
			
			if (report) {
				anyError(t);
			} else {
				Exceptions.onErrorDropped(t);
			}
		}
		
		@Override
		public void onComplete() {
			Map<Long, C> set;
			
			synchronized (this) {
				set = buffers;
				if (set == null) {
					return;
				}
			}
			
			cancelStart();
			cancelEnds();
			
			for (C b : set.values()) {
				queue.offer(b);
			}
			done = true;
			drain();
		}
		
		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.addAndGet(REQUESTED, this, n);
			}
		}
		
		void cancelMain() {
			BackpressureUtils.terminate(S, this);
		}
		
		void cancelStart() {
			starter.cancel();
		}
		
		void cancelEnds() {
			Set<Subscription> set;
			synchronized (starter) {
				set = endSubscriptions;
				
				if (set == null) {
					return;
				}
				endSubscriptions = null;
			}
			
			for (Subscription s : set) {
				s.cancel();
			}
		}
		
		boolean addEndSubscription(Subscription s) {
			synchronized (starter) {
				Set<Subscription> set = endSubscriptions;
				
				if (set != null) {
					set.add(s);
					return true;
				}
			}
			s.cancel();
			return false;
		}
		
		void removeEndSubscription(Subscription s) {
			synchronized (starter) {
				Set<Subscription> set = endSubscriptions;
				
				if (set != null) {
					set.remove(s);
					return;
				}
			}
		}
		
		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				
				cancelMain();
				
				cancelStart();
				
				cancelEnds();
			}
		}
		
		boolean emit(C b) {
			long r = requested;
			if (r != 0L) {
				actual.onNext(b);
				if (r != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return true;
			} else {
				
				actual.onError(new IllegalStateException("Could not emit buffer due to lack of requests"));
				
				return false;
			}
		}
		
		void anyError(Throwable t) {
			if (Exceptions.addThrowable(ERROR, this, t)) {
				done = true;
				drain();
			} else {
				Exceptions.onErrorDropped(t);
			}
		}
		
		void startNext(U u) {
			
			long idx = index;
			index = idx + 1;
			
			C b;

			try {
				b = bufferSupplier.get();
			} catch (Throwable e) {
				cancelStart();
				
				anyError(e);
				return;
			}
			
			if (b == null) {
				cancelStart();

				anyError(new NullPointerException("The bufferSupplier returned a null buffer"));
				return;
			}
			
			synchronized (this) {
				Map<Long, C> set = buffers;
				if (set == null) {
					return;
				}
				
				set.put(idx, b);
			}
			
			Publisher<V> p;
			
			try {
				p = end.apply(u);
			} catch (Throwable e) {
				cancelStart();
				
				anyError(e);
				return;
			}
			
			if (p == null) {
				cancelStart();

				anyError(new NullPointerException("The end returned a null publisher"));
				return;
			}
			
			StreamBufferStartEndEnder<T, V, C> end = new StreamBufferStartEndEnder<>(this, b, idx);
			
			if (addEndSubscription(end)) {
				OPEN.getAndIncrement(this);
				
				p.subscribe(end);
			}
		}
		
		void startError(Throwable e) {
			anyError(e);
		}
		
		void startComplete() {
			if (OPEN.decrementAndGet(this) == 0) {
				cancelAll();
				done = true;
				drain();
			}
		}
		
		void cancelAll() {
			cancelMain();
			
			cancelStart();
			
			cancelEnds();
		}
		
		void endSignal(StreamBufferStartEndEnder<T, V, C> ender) {
			synchronized (this) {
				Map<Long, C> set = buffers;
				
				if (set == null) {
					return;
				}
				
				if (set.remove(ender.index) == null) {
					return;
				}
				
				queue.offer(ender.buffer);
			}
			if (OPEN.decrementAndGet(this) == 0) {
				cancelAll();
				done = true;
			}
			drain();
		}
		
		void endError(Throwable e) {
			anyError(e);
		}
		
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			
			final Subscriber<? super C> a = actual;
			final Queue<C> q = queue;
			
			int missed = 1;
			
			for (;;) {
				
				for (;;) {
					boolean d = done;
					
					C b = q.poll();
					
					boolean empty = b == null;
					
					if (checkTerminated(d, empty, a, q)) {
						return;
					}
					
					if (empty) {
						break;
					}
					
					long r = requested;
					if (r != 0L) {
						actual.onNext(b);
						if (r != Long.MAX_VALUE) {
							REQUESTED.decrementAndGet(this);
						}
					} else {
						anyError(new IllegalStateException("Could not emit buffer due to lack of requests"));
						continue;
					}
				}
				
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
		
		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled) {
				queue.clear();
				return true;
			}
			
			if (d) {
				Throwable e = Exceptions.terminate(ERROR, this);
				if (e != null && e != Exceptions.TERMINATED) {
					cancel();
					queue.clear();
					a.onError(e);
					return true;
				} else
				if (empty) {
					a.onComplete();
					return true;
				}
			}
			return false;
		}
	}
	
	static final class StreamBufferStartEndStarter<U> extends DeferredSubscription
	implements Subscriber<U> {
		final StreamBufferStartEndMain<?, U, ?, ?> main;
		
		public StreamBufferStartEndStarter(StreamBufferStartEndMain<?, U, ?, ?> main) {
			this.main = main;
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
		}
		
		@Override
		public void onNext(U t) {
			main.startNext(t);
		}
		
		@Override
		public void onError(Throwable t) {
			main.startError(t);
		}
		
		@Override
		public void onComplete() {
			main.startComplete();
		}
	}
	
	static final class StreamBufferStartEndEnder<T, V, C extends Collection<? super T>> extends DeferredSubscription
	implements Subscriber<V> {
		final StreamBufferStartEndMain<T, ?, V, C> main;

		final C buffer;
		
		final long index;
		
		public StreamBufferStartEndEnder(StreamBufferStartEndMain<T, ?, V, C> main, C buffer, long index) {
			this.main = main;
			this.buffer = buffer;
			this.index = index;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(V t) {
			if (!isCancelled()) {
				cancel();
				
				main.endSignal(this);
			}
		}

		@Override
		public void onError(Throwable t) {
			main.endError(t);
		}

		@Override
		public void onComplete() {
			if (!isCancelled()) {
				main.endSignal(this);
			}
		}
		
	}
}
