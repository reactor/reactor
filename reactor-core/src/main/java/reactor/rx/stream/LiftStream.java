/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

import org.reactivestreams.Subscriber;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Function;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.action.CombineAction;

import javax.annotation.Nonnull;

/**
 * A Stream wrapper that defers a parent stream subscription to the child action subscribe() call.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class LiftStream<O, V> extends Stream<V> {
	private final Stream<O>                                                              producer;
	private final Function<? super Dispatcher, ? extends Action<? super O, ? extends V>> child;

	public LiftStream(Stream<O> thiz, Function<? super Dispatcher, ? extends Action<? super O, ? extends V>> action) {
		this.producer = thiz;
		this.child = action;
	}

	public final Action<? super O, ? extends V> onLift() {
		Action<? super O, ? extends V> action = child.apply(getDispatcher());
		if (action.getEnvironment() == null) {
			action.env(getEnvironment());
		}
		return action;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <E> CombineAction<E, V> combine() {
		Action<? super O, ? extends V> action = onLift();

		if(action == null){
			throw new IllegalStateException("Cannot combine streams without any lifted action");
		}

		Stream<?> parent = producer;
		producer.subscribe(action);

		while (parent != null &&
				LiftStream.class.isAssignableFrom(parent.getClass())
				) {
			parent = ((LiftStream) parent).producer;
		}
		if (parent != null && Action.class.isAssignableFrom(parent.getClass())) {
			return new CombineAction<E, V>((Action<E, ?>) parent, (Action<?, V>) action);
		} else {
			Action<E, V> sub = (Action<E, V>) action;
			return new CombineAction<E, V>(sub, sub);
		}
	}

	@Override
	public long getCapacity() {
		return producer.getCapacity();
	}

	@Override
	public Dispatcher getDispatcher() {
		return producer.getDispatcher();
	}

	@Override
	public Environment getEnvironment() {
		return producer.getEnvironment();
	}

	@Override
	public final Stream<V> dispatchOn(final Environment environment, final @Nonnull Dispatcher dispatcher) {
		final long capacity = dispatcher.backlogSize() != Long.MAX_VALUE ?
				dispatcher.backlogSize() - Action.RESERVED_SLOTS :
				Long.MAX_VALUE;

		return new LiftStream<O, V>(producer, child) {
			@Override
			public Dispatcher getDispatcher() {
				return dispatcher;
			}

			@Override
			public Environment getEnvironment() {
				return environment;
			}

			@Override
			public long getCapacity() {
				return capacity;
			}
		};
	}

	@Override
	public final void subscribe(Subscriber<? super V> s) {
		try {
			Action<? super O, ? extends V> action = onLift();

			action.subscribe(s);
			producer.subscribe(action);

		} catch (Throwable throwable) {
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
