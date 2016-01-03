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

/**
 * Expects and emits a single item at most from the source, IndexOutOfBoundsException for a multi-item source.
 *
 * @param <T> the value type
 */

/**
 * {@see https://github.com/reactor/reactive-streams-commons}
 * @since 2.5
 */
public final class MonoFirst<T> extends reactor.Mono.MonoBarrier<T, T> {

	public MonoFirst(Publisher<? extends T> source) {
		super(source);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new MonoSingle.MonoSingleSubscriber<>(s, MonoSingle.COMPLETE_ON_EMPTY_SEQUENCE));
	}

}
