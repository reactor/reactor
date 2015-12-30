/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.rx.stream;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Flux;
import reactor.Processors;
import reactor.Publishers;
import reactor.core.error.SpecificationExceptions;
import reactor.core.publisher.FluxFactory;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.core.timer.Timer;
import reactor.fn.Function;
import reactor.rx.Stream;
import reactor.rx.broadcast.StreamProcessor;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public class StreamBarrier<I, O> extends Stream<O>
		implements ReactiveState.Named, ReactiveState.Upstream, Flux.Operator<I, O>,
		           Publishers.LiftOperator<I, O> {

	final protected Publisher<I> source;

	public StreamBarrier(Publisher<I> source) {
		this.source = source;
	}

	@Override
	public void subscribe(Subscriber<? super O> s) {
		if (s == null) {
			throw SpecificationExceptions.spec_2_13_exception();
		}
		source.subscribe(apply(s));
	}

	@Override
	public long getCapacity() {
		return ReactiveState.Bounded.class.isAssignableFrom(source.getClass()) ?
				((ReactiveState.Bounded) source).getCapacity() : Long.MAX_VALUE;
	}

	@Override
	public Timer getTimer() {
		return Stream.class.isAssignableFrom(source.getClass()) ? ((Stream) source).getTimer() : super.getTimer();
	}

	@Override
	@SuppressWarnings("unchecked")
	public Subscriber<? super I> apply(Subscriber<? super O> subscriber) {
		return (Subscriber<I>)subscriber;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <E> StreamProcessor<E, O> combine() {
		Subscriber<?> oldestReceiver = null;
		Function oldestOperator = operator();

		Object oldestSender = this;

		while (oldestSender != null && Upstream.class.isAssignableFrom(oldestSender.getClass())) {
			oldestSender = ((Upstream) oldestSender).upstream();
			if (oldestSender != null) {
				if (Subscriber.class.isAssignableFrom(oldestSender.getClass())) {
					oldestReceiver = (Subscriber) oldestSender;
				}
				if (FluxFactory.LiftOperator.class.isAssignableFrom(oldestSender.getClass())) {
					oldestOperator =
							Publishers.<Object, Object, Object>opFusion(((FluxFactory.LiftOperator<Object, Object>) oldestSender).operator(),
									oldestOperator);
				}
			}
		}

		if (oldestReceiver == null) {
			Processor<E, E> root = Processors.emitter();
			return StreamProcessor.from(root, Publishers.lift(root, oldestOperator));
		}
		else {
			return StreamProcessor.from((Subscriber<E>) oldestReceiver, this);
		}
	}

	@Override
	public String getName() {
		Function operator = operator();
		return ReactiveStateUtils.getName(this == operator ? getClass().getSimpleName() : operator)
		                 .replaceAll("Stream|Publisher|Operator", "");
	}

	@Override
	public final Publisher<I> upstream() {
		return source;
	}

	@Override
	public Function<Subscriber<? super O>, Subscriber<? super I>> operator() {
		return this;
	}

	@Override
	public String toString() {
		return "{" +
				"source: " + source.toString() +
				'}';
	}

	public final static class Identity<I> extends StreamBarrier<I, I> {

		public Identity(Publisher<I> source) {
			super(source);
		}

		@Override
		public void subscribe(Subscriber<? super I> s) {
			source.subscribe(s);
		}
	}

	public final static class Operator<I, O> extends StreamBarrier<I, O> {

		private final Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider;

		public Operator(Publisher<I> source, Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider) {
			super(source);
			this.barrierProvider = barrierProvider;
		}

		@Override
		public Function<Subscriber<? super O>, Subscriber<? super I>> operator() {
			return barrierProvider;
		}

		@Override
		public Subscriber<? super I> apply(Subscriber<? super O> subscriber) {
			return barrierProvider.apply(subscriber);
		}
	}
}
