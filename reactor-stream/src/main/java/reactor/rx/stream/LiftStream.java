/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.error.Exceptions;
import reactor.core.support.Publishable;
import reactor.fn.Supplier;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.action.ProcessorAction;

import java.util.concurrent.locks.LockSupport;

/**
 * A Stream wrapper that defers a parent stream subscription to the child action subscribe() call.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class LiftStream<O, V> extends Stream<V> {
	private final Stream<O>                        producer;
	private final Supplier<? extends Action<O, V>> child;

	public LiftStream(Stream<O> thiz, Supplier<? extends Action<O, V>> action) {
		this.producer = thiz;
		this.child = action;
	}

	public final Action<O, V> onLift() {
		return child.get();
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <E> ProcessorAction<E, V> combine() {
		Action<O, V> action = onLift();

		if (action == null) {
			throw new IllegalStateException("Cannot combine streams without any lifted action");
		}

		producer.subscribe(action);
		while(action.getSubscription() == null){
			LockSupport.parkNanos(1L);
		}
		return action.combine();
	}

	@Override
	public long getCapacity() {
		return producer.getCapacity();
	}

	@Override
	public Timer getTimer() {
		return producer.getTimer();
	}

	@Override
	public final void subscribe(Subscriber<? super V> s) {
		try {
			Action<? super O, ? extends V> action = onLift();

			action.subscribe(s);
			producer.subscribe(action);

		} catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			s.onError(throwable);
		}
	}

	@Override
	public final String toString() {
		return "LiftStream{" +
		  "producer=" + producer.getClass().getSimpleName() +
		  '}';
	}
}
