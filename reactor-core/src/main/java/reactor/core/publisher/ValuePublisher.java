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
package reactor.core.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.support.ReactiveState;
import reactor.core.support.SignalType;
import reactor.fn.Supplier;

/**
 * @author Stephane Maldini
 */
public class ValuePublisher<IN> implements Publisher<IN>, Supplier<IN>, ReactiveState.FeedbackLoop {

	private final IN data;

	public ValuePublisher(IN data) {
		this.data = data;
	}

	@Override
	public void subscribe(final Subscriber<? super IN> s) {
		try {
			if(data == null){
				s.onSubscribe(SignalType.NOOP_SUBSCRIPTION);
				s.onComplete();
				return;
			}
			s.onSubscribe(new SingleSubscription<>(data, s));
		}
		catch (Throwable throwable) {
			Publishers.<IN>error(throwable).subscribe(s);
		}
	}

	@Override
	public Object delegateInput() {
		return null;
	}

	@Override
	public Object delegateOutput() {
		return data;
	}

	@Override
	public IN get() {
		return data;
	}

	@Override
	public String toString() {
		return "{ singleValue: \"" + data +"\" }";
	}

	private static class SingleSubscription<IN> implements Subscription, Upstream {

		private final Subscriber<? super IN> s;
		private final IN data;
		boolean terminado;

		public SingleSubscription(IN data, Subscriber<? super IN> s) {
			this.s = s;
			this.data = data;
		}

		@Override
		public void request(long elements) {
			if (terminado) {
				return;
			}

			terminado = true;
			s.onNext(data);
			s.onComplete();
		}

		@Override
		public void cancel() {
			terminado = true;
		}

		@Override
		public Object upstream() {
			return data;
		}
	}
}
