/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
import reactor.fn.Supplier;
import reactor.rx.Stream;

/**
 * A {@link org.reactivestreams.Publisher} supplier that will call the passed supplier on each subscribe call.
 * <p>

 * Create such stream with the provided factory, E.g.:
 * <pre>
 * {@code
 * Streams.defer(() -> Streams.just(random.nextInt()))
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
public class DeferredStream<T> extends Stream<T> {

	private final Supplier<? extends Publisher<T>> sourceFactory;

	public DeferredStream(Supplier<? extends Publisher<T>> sourceFactory) {
		this.sourceFactory = sourceFactory;
	}

	@Override
	public void subscribe(final Subscriber<? super T> subscriber) {
		try{
			sourceFactory.get().subscribe(subscriber);
		}catch (Throwable t){
			subscriber.onError(t);
		}
	}
}
