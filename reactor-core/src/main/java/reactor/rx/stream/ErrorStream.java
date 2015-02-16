/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package reactor.rx.stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.Stream;

/**
 * A Stream that emits a sigle error signal.
 * <p>
 * Since the stream retains the error in a final field, any {@link this#subscribe(org.reactivestreams.Subscriber)}
 * will replay the error. This is a "Cold" stream.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * <pre>
 * {@code
 * Streams.error(error).when(Throwable.class, log::error)
 * }
 * </pre>
 * <pre>
 * Will log:
 * {@code
 * 1
 * 2
 * 3
 * 4
 * complete
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
public final class ErrorStream<O, T extends Throwable> extends Stream<O> {

	final static private Subscription ERROR_SUB = new Subscription() {
		@Override
		public void request(long n) {
		}

		@Override
		public void cancel() {
		}
	};

	final private T error;

	public ErrorStream(T value) {
		this.error = value;
	}

	@Override
	public String toString() {
		return super.toString() + " " + error;
	}

	@Override
	public void subscribe(final Subscriber<? super O> s) {
		s.onSubscribe(ERROR_SUB);
		s.onError(error);
	}
}
