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

package reactor.io.net.http;

import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.support.Bounded;
import reactor.fn.Function;
import reactor.io.net.http.model.Status;
import reactor.io.net.http.model.Transfer;

/**
 * An HTTP {@link HttpChannel} extension that provides Headers status check and optional
 *  params resolution
 *
 * @author Sebastien Deleuze
 * @author Stephane maldini
 */
public abstract class BaseHttpChannel<IN, OUT> implements Bounded, HttpChannel<IN,OUT> {

	private volatile int statusAndHeadersSent = 0;
	private Function<? super String, Map<String, Object>> paramsResolver;

	private final long prefetch;

	protected final static AtomicIntegerFieldUpdater<BaseHttpChannel> HEADERS_SENT =
			AtomicIntegerFieldUpdater.newUpdater(BaseHttpChannel.class, "statusAndHeadersSent");

	public BaseHttpChannel(long prefetch) {
		this.prefetch = prefetch;
	}

	// REQUEST contract

	/**
	 * Read all URI params
	 * @return a map of resolved parameters against their matching key name
	 */
	@Override
	public Map<String, Object> params() {
		return null != paramsResolver ? paramsResolver.apply(uri()) : null;
	}

	/**
	 * Read URI param from the given key
	 * @param key matching key
	 * @return the resolved parameter for the given key name
	 */
	@Override
	public Object param(String key) {
		Map<String, Object> params = null;
		if (paramsResolver != null) {
			params = this.paramsResolver.apply(uri());
		}
		return null != params ? params.get(key) : null;
	}

	/**
	 * Register an HTTP request header
	 * @param name Header name
	 * @param value Header content
	 * @return this
	 */
	@Override
	public HttpChannel<IN, OUT> header(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doAddHeader(name, value);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	protected abstract void doHeader(String name, String value);

	/**
	 * Accumulate a Request Header using the given name and value, appending ";" for each
	 * new value
	 * @return this
	 */
	@Override
	public HttpChannel<IN, OUT> addHeader(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doAddHeader(name, value);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	protected abstract void doAddHeader(String name, String value);

	/**
	 *
	 * @param headerResolver
	 */
	@Override
	public HttpChannel<IN, OUT> paramsResolver(
			Function<? super String, Map<String, Object>> headerResolver) {
		this.paramsResolver = headerResolver;
		return this;
	}

	// RESPONSE contract

	/**
	 * Set the response status to an outgoing response
	 * @param status the status to define
	 * @return this
	 */
	@Override
	public HttpChannel<IN, OUT> responseStatus(Status status) {
		if (statusAndHeadersSent == 0) {
			doResponseStatus(status);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	protected abstract void doResponseStatus(Status status);

	/**
	 * Define the response HTTP header for the given key
	 * @param name the HTTP response header key to override
	 * @param value the HTTP response header content
	 * @return this
	 */
	@Override
	public HttpChannel<IN, OUT> responseHeader(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doResponseHeader(name, value);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	protected abstract void doResponseHeader(String name, String value);

	/**
	 * Accumulate a response HTTP header for the given key name, appending ";" for each
	 * new value
	 * @param name the HTTP response header name
	 * @param value the HTTP response header value
	 * @return this
	 */
	@Override
	public HttpChannel<IN, OUT> addResponseHeader(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doAddResponseHeader(name, value);
		}
		else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	/**
	 * Flush the headers if not sent. Might be useful for the case
	 * @return Stream to signal error or successful write to the client
	 */
	@Override
	public Publisher<Void> writeHeaders() {
		if (statusAndHeadersSent == 0) {
			return new Publisher<Void>() {
				@Override
				public void subscribe(Subscriber<? super Void> s) {
					if (markHeadersAsFlushed()) {
						doSubscribeHeaders(s);
					}
					else {
						Publishers.<Void>error(new IllegalStateException("Status and headers already sent"))
						          .subscribe(s);
					}
				}
			};
		}
		else {
			return Publishers.empty();
		}
	}

	/**
	 * @return the Transfer setting SSE for this http connection (e.g. event-stream)
	 */
	@Override
	public HttpChannel<IN, OUT> sse() {
		return transfer(Transfer.EVENT_STREAM);
	}

	protected abstract void doAddResponseHeader(String name, String value);

	protected boolean markHeadersAsFlushed() {
		return HEADERS_SENT.compareAndSet(this, 0, 1);
	}

	protected abstract void doSubscribeHeaders(Subscriber<? super Void> s);

	protected abstract Publisher<Void> writeWithAfterHeaders(Publisher<? extends OUT> s);

	@Override
	public Publisher<Void> writeWith(final Publisher<? extends OUT> source) {
		return new Publisher<Void>() {
			@Override
			public void subscribe(final Subscriber<? super Void> s) {
				if(markHeadersAsFlushed()){
					doSubscribeHeaders(new Subscriber<Void>() {
						@Override
						public void onSubscribe(Subscription sub) {
							sub.request(Long.MAX_VALUE);
						}

						@Override
						public void onNext(Void aVoid) {
							//Ignore
						}

						@Override
						public void onError(Throwable t) {
							s.onError(t);
						}

						@Override
						public void onComplete() {
							writeWithAfterHeaders(source).subscribe(s);
						}
					});
				}
				else{
					writeWithAfterHeaders(source).subscribe(s);
				}
			}
		};
	}

	@Override
	public boolean isExposedToOverflow(Bounded parentPublisher) {
		return false;
	}

	@Override
	public long getCapacity() {
		return prefetch;
	}
}
