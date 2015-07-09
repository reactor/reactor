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
package reactor.core.reactivestreams;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.fn.Function;

/**
 * A logging interceptor that intercepts all reactive calls and trace them
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public final class LogPublisher<IN> implements Publisher<IN> {

    /**
     *
     * @param publisher
     * @param <IN>
     * @return
     */
    public static <IN> Publisher<IN> log(Publisher<? extends IN> publisher){
        return log(publisher, null);
    }

    /**
     *
     * @param publisher
     * @param category
     * @param <IN>
     * @return
     */
    public static <IN> Publisher<IN> log(Publisher<? extends IN> publisher, String category){
        return new LogPublisher<>(publisher, category);
    }


    private final Publisher<IN> wrappedPublisher;

    protected LogPublisher(final Publisher<? extends IN> source,
                           final String category) {

        final Logger log = category != null && !category.isEmpty() ?
          LoggerFactory.getLogger(category) :
          LoggerFactory.getLogger(LogPublisher.class);

        this.wrappedPublisher = PublisherFactory.intercept(
          source,
          new Function<Subscriber<? super IN>, SubscriberBarrier<IN, IN>>() {
              @Override
              public SubscriberBarrier<IN, IN> apply(Subscriber<? super IN> subscriber) {
                  if(log.isTraceEnabled()) {
                      log.trace("subscribe: {}", subscriber.getClass().getSimpleName());
                  }
                  return new LoggerBarrier<>(log, subscriber);
              }
          });
    }

    @Override
    public void subscribe(Subscriber<? super IN> s) {
        wrappedPublisher.subscribe(s);
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
            if(log.isInfoEnabled()){
                log.info("onSubscribe({})", this.subscription);
            }
            super.doOnSubscribe(subscription);
        }

        @Override
        protected void doNext(IN in) {
            if(log.isInfoEnabled()){
                log.info("onNext({})", in);
            }
            super.doNext(in);
        }

        @Override
        protected void doError(Throwable throwable) {
            if(log.isErrorEnabled()){
                log.error("onError({})", throwable);
            }
            super.doError(throwable);
        }

        @Override
        protected void doComplete() {
            if(log.isInfoEnabled()){
                log.info("onComplete()");
            }
            super.doComplete();
        }

        @Override
        protected void doRequest(long n) {
            if(log.isInfoEnabled()){
                log.info("request({})", n);
            }
            super.doRequest(n);
        }

        @Override
        protected void doCancel() {
            if(log.isInfoEnabled()){
                log.info("cancel()");
            }
            super.doCancel();
        }



        @Override
        public String toString() {
            return super.toString()+"{logger="+log.getName()+"}";
        }
    }

}
