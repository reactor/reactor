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
package reactor.rx.stream.io;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.codec.Codec;
import reactor.rx.Stream;
import reactor.rx.action.support.DefaultSubscriber;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public final class DecoderStream<SRC, IN> extends Stream<IN> {

	final private Codec<SRC, IN, ?> codec;
	final private Publisher<? extends SRC> publisher;

	public DecoderStream(Codec<SRC, IN, ?> codec, Publisher<? extends SRC> publisher) {
		this.codec = codec;
		this.publisher = publisher;
	}

	@Override
	public void subscribe(final Subscriber<? super IN> subscriber) {
		final Function<SRC, IN> decoder = codec.decoder(new Consumer<IN>() {
			@Override
			public void accept(IN in) {
				subscriber.onNext(in);
			}
		});

		publisher.subscribe(new DefaultSubscriber<SRC>() {
			@Override
			public void onSubscribe(final Subscription s) {
				subscriber.onSubscribe(new Subscription() {
					@Override
					public void request(long n) {
						s.request(n);
					}

					@Override
					public void cancel() {
						s.cancel();
					}
				});
			}

			@Override
			public void onNext(SRC src) {
				decoder.apply(src);
			}

			@Override
			public void onError(Throwable t) {
				subscriber.onError(t);
			}

			@Override
			public void onComplete() {
				subscriber.onComplete();

			}
		});
	}
}
