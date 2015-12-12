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
import reactor.Processors;
import reactor.Publishers;
import reactor.core.error.SpecificationExceptions;
import reactor.core.publisher.PublisherFactory;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.action.StreamProcessor;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class StreamOperator<I, O> extends Stream<O> implements ReactiveState.Named, ReactiveState.Upstream,
                                                                     PublisherFactory.LiftOperator<I, O>{

	final private Publisher<I>                                           source;
	final private Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider;

	public StreamOperator(Publisher<I> source, Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider) {
		this.source = source;
		this.barrierProvider = barrierProvider;
	}

	@Override
	public void subscribe(Subscriber<? super O> s) {
		if (s == null) {
			throw SpecificationExceptions.spec_2_13_exception();
		}
		source.subscribe(barrierProvider.apply(s));
	}

	@Override
	public long getCapacity() {
		return ReactiveState.Bounded.class.isAssignableFrom(source.getClass()) ? ((ReactiveState.Bounded) source).getCapacity() : Long.MAX_VALUE;
	}

	@Override
	public Timer getTimer() {
		return Stream.class.isAssignableFrom(source.getClass()) ? ((Stream) source).getTimer() : super.getTimer();
	}

	@Override
	@SuppressWarnings("unchecked")
	public <E> StreamProcessor<E, O> combine() {
		Subscriber<?> oldestReceiver = null;
		Function oldestOperator = barrierProvider;

		Object oldestSender = this;

		while( oldestSender != null && Upstream.class.isAssignableFrom(oldestSender.getClass())){
			oldestSender = ((Upstream)oldestSender).upstream();
			if (oldestSender != null){
				if(Subscriber.class.isAssignableFrom(oldestSender.getClass())){
					oldestReceiver = (Subscriber)oldestSender;
				}
				if(PublisherFactory.LiftOperator.class.isAssignableFrom(oldestSender.getClass())){
					oldestOperator = Publishers.<Object, Object, Object>opFusion(
							((PublisherFactory.LiftOperator<Object, Object>)oldestSender).operator(),
							oldestOperator
					);
				}
			}
		}

		if (oldestReceiver == null){
			Processor<E, E> root = Processors.emitter();
			return StreamProcessor.wrap(
					root,
					Publishers.lift(root, oldestOperator)
			);
		}
		else{
			return StreamProcessor.wrap(
					(Subscriber<E>)oldestReceiver,
					this
			);
		}
	}

	@Override
	public String getName() {
		return ReactiveStateUtils.getName(barrierProvider).replaceAll("Operator", "");
	}

	@Override
	public Object upstream() {
		return source;
	}

	@Override
	public Function<Subscriber<? super O>, Subscriber<? super I>> operator() {
		return barrierProvider;
	}

	@Override
	public String toString() {
		return "{" +
				"lift: true, " +
				"source: " + source.toString() +
				'}';
	}
}
