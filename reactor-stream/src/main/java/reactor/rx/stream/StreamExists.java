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
package reactor.rx.stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Predicate;

/**
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 *
 * @since 2.0, 2.1
 */
public final class StreamExists<T> extends StreamBarrier<T, Boolean> {

	private final Predicate<? super T> p;

	public StreamExists(Publisher<T> source, Predicate<? super T> predicate) {
		super(source);
		this.p = predicate;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super Boolean> subscriber) {
		return new ExistsAction<>(subscriber, p);
	}

    final static class ExistsAction<T> extends SubscriberBarrier<T, Boolean> {

        private final Predicate<? super T> predicate;

        private boolean elementFound;

        public ExistsAction(Subscriber<? super Boolean> subscriber, Predicate<? super T> predicate) {
	        super(subscriber);
            this.predicate = predicate;
        }

        @Override
        protected void doNext(T ev) {
            if (predicate.test(ev)) {
                elementFound = true;
                cancel();
                subscriber.onNext(true);
                onComplete();
            }
        }

        @Override
        protected void doComplete() {
            if (!elementFound) {
	            subscriber.onNext(false);
            }
            super.doComplete();
        }

    }

}
