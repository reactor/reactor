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
package reactor.core.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Mono;
import reactor.core.error.Exceptions;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.ReactiveState;

/**
 * @author Stephane Maldini
 */
public final class MonoError<IN> extends Mono<IN> implements ReactiveState.FailState {

	/**
	 * Return a failed {@link Publisher} if the given error is not fatal
	 * @param error the error to {link Subscriber#onError}
	 * @param <IN> the nominal type flowing through
	 * @return a failed {@link Publisher}
	 */
	public static <IN> Mono<IN> create(final Throwable error) {
		Exceptions.throwIfFatal(error);
		return new MonoError<>(error);
	}

	private final Throwable error;

	public MonoError(Throwable error) {
		this.error = error;
	}

	@Override
	public void subscribe(Subscriber<? super IN> s) {
		if (s == null) {
			throw Exceptions.spec_2_13_exception();
		}
		s.onSubscribe(EmptySubscription.INSTANCE);
		s.onError(error);
	}

	@Override
	public Throwable getError() {
		return error;
	}
}
