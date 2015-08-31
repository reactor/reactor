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
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public final class LogOperator<IN> implements Function<Subscriber<? super IN>, Subscriber<? super IN>>{

	private final Logger log;

	public LogOperator(final String category) {

		this.log = category != null && !category.isEmpty() ?
		  LoggerFactory.getLogger(category) :
		  LoggerFactory.getLogger(LogOperator.class);

	}

	@Override
	public Subscriber<? super IN> apply(Subscriber<? super IN> subscriber) {
		if (log.isTraceEnabled()) {
			log.trace("subscribe: {}", subscriber.getClass().getSimpleName());
		}
		return new LoggerBarrier<>(log, subscriber);
	}

	private static class LoggerBarrier<IN> extends SubscriberBarrier<IN, IN> {

		private final Logger log;

		public LoggerBarrier(Logger log,
		                     Subscriber<? super IN> subscriber) {
			super(subscriber);
			this.log = log;
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			if (log.isInfoEnabled()) {
				log.info("⇩ onSubscribe({})", this.subscription);
			}
			super.doOnSubscribe(subscription);
		}

		@Override
		protected void doNext(IN in) {
			if (log.isInfoEnabled()) {
				log.info("↓ onNext({})", in);
			}
			super.doNext(in);
		}

		@Override
		protected void doError(Throwable throwable) {
			if (log.isErrorEnabled()) {
				log.error("↯ onError({})", throwable);
			}
			super.doError(throwable);
		}

		@Override
		protected void doComplete() {
			if (log.isInfoEnabled()) {
				log.info("↧ onComplete()");
			}
			super.doComplete();
		}

		@Override
		protected void doRequest(long n) {
			if (log.isInfoEnabled()) {
				log.info("⇡ request({})", n);
			}
			super.doRequest(n);
		}

		@Override
		protected void doCancel() {
			if (log.isInfoEnabled()) {
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
