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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.event.dispatch.Dispatcher;

/**
 * @author Stephane Maldini
 */
public class LoggerAction<T> extends Action<T, T> {

	private final Logger log;

	public LoggerAction(Dispatcher dispatcher, String logger) {
		super(dispatcher);
		log = logger != null && !logger.isEmpty() ? LoggerFactory.getLogger(logger) : LoggerFactory.getLogger(LoggerAction
				.class);
	}

	@Override
	protected void doNext(T ev) {
		log.info("onNext: {}", ev);
		broadcastNext(ev);
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		log.info("subscribe: {}-{}", subscriber.getClass().getSimpleName(), subscriber);
		super.subscribe(subscriber);
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		log.info("onSubscribe: {}", subscription);
		super.doSubscribe(subscription);
	}

	@Override
	protected void doError(Throwable ev) {
		log.error("onError: {}", ev);
		super.doError(ev);
	}

	@Override
	protected void onRequest(long n) {
		log.info("request: {}", n);
//		if(log.isDebugEnabled()){
//			log.debug("stream: {}", debug());
//		}
		super.onRequest(n);
	}

	@Override
	public Action<T, T> cancel() {
		if (upstreamSubscription != null && upstreamSubscription.getPublisher() != null) {
			log.info("cancel: {}-{}", this.upstreamSubscription.getPublisher().getClass().getSimpleName(),
					this.upstreamSubscription.getPublisher());
		} else {
			log.info("cancel");
		}
		return super.cancel();
	}

	@Override
	protected void doComplete() {
		if (upstreamSubscription != null && upstreamSubscription.getPublisher() != null) {
			log.info("complete: {}-{}", this.upstreamSubscription.getPublisher().getClass().getSimpleName(),
					this.upstreamSubscription.getPublisher());
		} else {
			log.info("complete");
		}
		super.doComplete();
	}
}
