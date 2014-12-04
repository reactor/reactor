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
 * An interface generally associated with a {@link Stream} terminal action such as
 * {@link Stream#consume(reactor.function.Consumer)}
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public interface Controls {

	/**
	 * Request the next n elements from the source
	 * @param n the number of elements to request
	 */
	void requestMore(long n);

	/**
	 * Usually requests Long.MAX_VALUE, which instructs a stream to never end until completed or cancelled.
	 */
	void start();

	/**
	 * Stop consuming signals from upstream. Cancel should not be considered blocking, but usually it happens to be
	 * rather immediate as it will be updating {@link reactor.rx.subscription.PushSubscription#terminated} flag.
	 */
	void cancel();

	/**
	 * Block the consumer until it has received a terminal event (complete or error). If an error occurs, it will be propagated to the caller.
	 * If no previous request has been made, it will automatically ask for every element (Long.MAX_VALUE)
	 */
	void block() throws Throwable;

	/**
	 * Check if the current stream is emitting any signal.
	 */
	boolean isPublishing();


	/**
	 * Parse the materialized upstream source to fetch a materialized map form which allows for graph-style printing.
	 * @return {@link reactor.rx.StreamUtils.StreamVisitor} a Debug container for the current source
	 */
	StreamUtils.StreamVisitor debug();
}
