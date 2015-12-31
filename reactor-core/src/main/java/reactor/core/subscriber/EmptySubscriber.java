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
package reactor.core.subscriber;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public enum EmptySubscriber implements Subscriber<Object> {
    INSTANCE;

    @SuppressWarnings("unchecked")
    public static <T> Subscriber<T> instance() {
        return (Subscriber<T>) INSTANCE;
    }

    @Override
    public void onSubscribe(Subscription s) {
        // deliberately no op
    }

    @Override
    public void onNext(Object t) {
        // deliberately no op
    }

    @Override
    public void onError(Throwable t) {
        // deliberately no op
    }

    @Override
    public void onComplete() {
        // deliberately no op
    }

}
