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
package reactor.rx;

/**
 * A domain representation of a Reactive {@link Stream} signal.
 * There are 4 differents signals and their possible sequence is defined as such:
 * onError | (onSubscribe onNext* (onError | onComplete)?)
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public enum Signal {
	/**
	 * Only happens once, a subscribe signal is the handshake between a new subscriber and a producer.
	 *
	 * see {@link Stream#subscribe(org.reactivestreams.Subscriber)}
	 */
	SUBSCRIBE,

	/**
	 * Can happen N times where N is a possibly unbounded number. The signal will trigger core logic on all
	 * {@link reactor.rx.action.Action} attached to a {@link Stream}.
	 *
	 * see {@link reactor.rx.action.Action#onNext(Object)}
	 */
	NEXT,

	/**
	 * Only happens once, a complete signal is used to confirm the successful end of the data sequence flowing in a
	 * {@link Stream}. The signal releases batching operations such as {@link Stream#buffer()},
	 * {@link Stream#window()} or {@link Stream#reduce(reactor.function.Function)}
	 *
	 * see {@link reactor.rx.action.Action#onComplete()}
	 */
	COMPLETE,

	/**
	 * Only happens once, a complete signal is used to confirm the error end of the data sequence flowing in a
	 * {@link Stream}. However, the signal can be recovered using various operations such as {@link Stream#recover
	 * (Class)} or {@link reactor.rx.Stream#retry()}
	 *
	 * see {@link reactor.rx.action.Action#onError(Throwable cause)}
	 */
	ERROR
}
