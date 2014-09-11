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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FanOutSubscription<O> extends StreamSubscription<O> {

	static final Logger log = LoggerFactory.getLogger(FanOutSubscription.class);

	private final List<StreamSubscription<O>> subscriptions = new ArrayList<StreamSubscription<O>>(2);
	private final ReentrantReadWriteLock      lock          = new ReentrantReadWriteLock();

	public FanOutSubscription(Stream<O> publisher, StreamSubscription<O> streamSubscriptionA,
	                          StreamSubscription<O> streamSubscriptionB) {
		super(publisher, null, null);
		subscriptions.add(streamSubscriptionA);
		subscriptions.add(streamSubscriptionB);
	}

	@Override
	public void onComplete() {
		forEach(new Consumer<StreamSubscription<O>>() {
			@Override
			public void accept(StreamSubscription<O> subscription) {
				try {
					subscription.onComplete();
				} catch (Throwable throwable) {
					subscription.onError(throwable);
				}
			}
		});
	}

	@Override
	public void onNext(final O ev) {

		forEach(new Consumer<StreamSubscription<O>>() {
			@Override
			public void accept(StreamSubscription<O> subscription) {
				try {
					if (subscription.isComplete()) {
						if (log.isDebugEnabled()) {
							log.debug("event ignored [" + ev + "] as downstream subscription is complete");
						}
						return;
					}

					subscription.onNext(ev);

				} catch (Throwable throwable) {
					subscription.onError(throwable);
				}
			}
		});
	}

	@Override
	public void cancel() {
		forEach(new Consumer<StreamSubscription<O>>() {
			@Override
			public void accept(StreamSubscription<O> oStreamSubscription) {
				oStreamSubscription.cancel();
			}
		});
		super.cancel();
	}

	@Override
	public void onError(final Throwable ev) {
		forEach(new Consumer<StreamSubscription<O>>() {
			@Override
			public void accept(StreamSubscription<O> oStreamSubscription) {
				oStreamSubscription.onError(ev);
			}
		});
	}

	@Override
	public boolean isComplete() {
		lock.readLock().lock();
		try {
			boolean isComplete = false;
			for(StreamSubscription<O> subscription : subscriptions){
				isComplete = subscription.isComplete();
				if(!isComplete) break;
			}
			return isComplete;
		} finally {
			lock.readLock().unlock();
		}
	}

	public void forEach(Consumer<StreamSubscription<O>> consumer) {
		lock.readLock().lock();
		try {
			for(StreamSubscription<O> subscription : subscriptions){
				if(subscription != null){
					consumer.accept(subscription);
				}
			}
		} finally {
			lock.readLock().unlock();
		}
	}

	public List<StreamSubscription<O>> getSubscriptions() {
		return Collections.unmodifiableList(subscriptions);
	}

	public boolean isEmpty(){
		lock.readLock().lock();
		try{
			return subscriptions.isEmpty();
		} finally {
			lock.readLock().unlock();
		}
	}

	public boolean remove(StreamSubscription<O> subscription) {
		lock.writeLock().lock();
		try {
			return subscriptions.remove(subscription);
		} finally {
			lock.writeLock().unlock();
		}
	}


	public boolean add(StreamSubscription<O> subscription) {
		lock.writeLock().lock();
		try {
			return subscriptions.add(subscription);
		} finally {
			lock.writeLock().unlock();
		}
	}

	public boolean contains(StreamSubscription<O> subscription) {
		lock.readLock().lock();
		try{
			return subscriptions.contains(subscription);
		} finally {
			lock.readLock().unlock();
		}
	}
}
