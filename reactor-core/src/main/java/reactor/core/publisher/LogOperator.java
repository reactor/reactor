/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Function;

/**
 * A logging interceptor that intercepts all reactive calls and trace them
 * @author Stephane Maldini
 * @since 2.1
 */
public final class LogOperator<IN>
		implements Function<Subscriber<? super IN>, Subscriber<? super IN>> {

	public static final int SUBSCRIBE    = 0b01000000;
	public static final int ON_SUBSCRIBE = 0b00100000;
	public static final int ON_NEXT      = 0b00010000;
	public static final int ON_ERROR     = 0b00001000;
	public static final int ON_COMPLETE  = 0b00000100;
	public static final int REQUEST      = 0b00000010;
	public static final int CANCEL       = 0b00000001;
	public static final int TERMINAL     = CANCEL | ON_COMPLETE | ON_ERROR;

	public static final int ALL = 0b1111111;

	private final Logger log;

	private final int options;

	public LogOperator(final String category, int options) {

		this.log = category != null && !category.isEmpty() ?
				LoggerFactory.getLogger(category) :
				LoggerFactory.getLogger(LogOperator.class);
		this.options = options;
	}

	@Override
	public Subscriber<? super IN> apply(Subscriber<? super IN> subscriber) {
		if ((options & SUBSCRIBE) == SUBSCRIBE && log.isInfoEnabled()) {
			log.trace("subscribe: {}", subscriber.getClass().getSimpleName());
		}
		return new LoggerBarrier<>(log, subscriber, options);
	}

	private static class LoggerBarrier<IN> extends SubscriberBarrier<IN, IN> {

		private final int    options;
		private final Logger log;

		public LoggerBarrier(Logger log, Subscriber<? super IN> subscriber, int options) {
			super(subscriber);
			this.log = log;
			this.options = options;
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			if ((options & ON_SUBSCRIBE) == ON_SUBSCRIBE && log.isInfoEnabled()) {
				log.info("⇩ onSubscribe({})", this.subscription);
			}
			super.doOnSubscribe(subscription);
		}

		@Override
		protected void doNext(IN in) {
			if ((options & ON_NEXT) == ON_NEXT && log.isInfoEnabled()) {
				log.info("↓ onNext({})", in);
			}
			super.doNext(in);
		}

		@Override
		protected void doError(Throwable throwable) {
			if ((options & ON_ERROR) == ON_ERROR && log.isErrorEnabled()) {
				log.error("↯ onError({})", throwable);
			}
			super.doError(throwable);
		}

		@Override
		protected void doComplete() {
			if ((options & ON_COMPLETE) == ON_COMPLETE && log.isInfoEnabled()) {
				log.info("↧ onComplete()");
			}
			super.doComplete();
		}

		@Override
		protected void doRequest(long n) {
			if ((options & REQUEST) == REQUEST && log.isInfoEnabled()) {
				log.info("⇡ request({})", Long.MAX_VALUE == n ? "unbounded" : n);
			}
			super.doRequest(n);
		}

		@Override
		protected void doCancel() {
			if ((options & CANCEL) == CANCEL && log.isInfoEnabled()) {
				log.info("↥ cancel()");
			}
			super.doCancel();
		}

		@Override
		public String toString() {
			return "{logger=" + log.getName() + "}";
		}
	}

}
