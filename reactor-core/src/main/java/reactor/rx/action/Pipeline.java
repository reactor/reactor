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
package reactor.rx.action;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.event.lifecycle.Pausable;

import javax.annotation.Nonnull;

/**
 * A Data publisher that is fulfilling the Reactive-Streams TCK.
 * This component supports flush operation mainly involved in batching and buffer release.
 * A pipeline such as {@link reactor.rx.Stream} offers primitives to manipulate data and eventually to produce an
 * output. It uses an "happen-before" strategy meaning that the current pipeline processing logic will be executed
 * before passing the data to the subscribed actions.
 *
 * @param <O> the data output type
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1
 */
public interface Pipeline<O> extends Publisher<O>, Pausable {

	/**
	 * Subscribe an {@link Action} to the actual pipeline. Additionally to producing events (error,complete,next and
	 * eventually flush), it will generally take care of setting the environment if available and
	 * an initial prefetch size used for {@link org.reactivestreams.Subscription#request(int)}.
	 * Reactive Extensions patterns also dubs this operation "lift".
	 *
	 * @param action the processor to subscribe.
	 * @param <E> the {@param action} output type
	 *
	 * @return the current {link Pipeline} instance
	 *
	 * @see {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)}
	 * @since 1.1
	 */
	<A,E extends Action<O,A>> E connect(@Nonnull E action);

	/**
	 * Trigger a flush event to all the attached {@link Subscriber} that implement {@link Flushable},
	 * e.g. {@link CollectAction} or {@link SupplierAction}
	 *
	 * @since 1.1
	 */
	void broadcastFlush();

	/**
	 * Send an error to all the attached {@link Subscriber}.
	 *
	 * @param throwable the error to forward
	 *
	 * @since 1.1
	 */
	void broadcastError(Throwable throwable);

	/**
	 * Send an element of parameterized type {link O} to all the attached {@link Subscriber}.
	 *
	 * @param data the data to forward
	 *
	 * @since 1.1
	 */
	void broadcastNext(O data);

	/**
	 * Send a complete event to all the attached {@link Subscriber}.
	 *
	 * @since 1.1
	 */
	void broadcastComplete();
}
