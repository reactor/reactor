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

package reactor.io.net.http;

import reactor.Environment;
import reactor.bus.selector.HeaderResolver;
import reactor.core.Dispatcher;
import reactor.io.net.ChannelStream;
import reactor.io.net.http.model.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author Sebastien Deleuze
 * @author Stephane maldini
 */
public abstract class HttpChannel<IN, OUT> extends ChannelStream<IN, OUT> {

	private volatile int statusAndHeadersSent = 0;
	private HeaderResolver<String> paramsResolver;

	protected final static AtomicIntegerFieldUpdater<HttpChannel> HEADERS_SENT =
			AtomicIntegerFieldUpdater.newUpdater(HttpChannel.class, "statusAndHeadersSent");

	public HttpChannel(Environment env,
	                   long prefetch,
	                   Dispatcher eventsDispatcher
	) {
		super(env, null, prefetch, eventsDispatcher);
	}

	// REQUEST contract

	/**
	 * Read all URI params
	 *
	 * @return
	 */
	public final Map<String, String> params() {
		return null != paramsResolver ? paramsResolver.resolve(uri()) : null;
	}

	/**
	 * Read URI param from the given key
	 *
	 * @param key
	 * @return
	 */
	public final String param(String key) {
		Map<String, String> params = null;
		if (paramsResolver != null) {
			params = this.paramsResolver.resolve(uri());
		}
		return null != params ? params.get(key) : null;
	}

	/**
	 * @return
	 */
	public abstract HttpHeaders headers();

	/**
	 * @param name
	 * @param value
	 * @return this
	 */
	public final HttpChannel<IN, OUT> header(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doHeader(name, value);
		} else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	protected abstract void doHeader(String name, String value);

	/**
	 * @param name
	 * @param value
	 * @return this
	 */
	public HttpChannel<IN, OUT> addHeader(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doAddHeader(name, value);
		} else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	protected abstract void doAddHeader(String name, String value);

	/**
	 * @return
	 */
	public abstract Protocol protocol();

	/**
	 * @return
	 */
	public abstract String uri();

	/**
	 * @return
	 */
	public abstract Method method();


	void paramsResolver(HeaderResolver<String> headerResolver) {
		this.paramsResolver = headerResolver;
	}

	// RESPONSE contract

	/**
	 * @return
	 */
	public abstract Status responseStatus();

	/**
	 * @param status
	 * @return this
	 */
	public HttpChannel<IN, OUT> responseStatus(Status status) {
		if (statusAndHeadersSent == 0) {
			doResponseStatus(status);
		} else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	protected abstract void doResponseStatus(Status status);

	/**
	 * @return
	 */
	public abstract ResponseHeaders responseHeaders();

	/**
	 * @param name
	 * @param value
	 * @return this
	 */
	public final HttpChannel<IN, OUT> responseHeader(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doResponseHeader(name, value);
		} else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	protected abstract void doResponseHeader(String name, String value);

	/**
	 * @param name
	 * @param value
	 * @return this
	 */
	public HttpChannel<IN, OUT> addResponseHeader(String name, String value) {
		if (statusAndHeadersSent == 0) {
			doAddResponseHeader(name, value);
		} else {
			throw new IllegalStateException("Status and headers already sent");
		}
		return this;
	}

	protected abstract void doAddResponseHeader(String name, String value);

	/**
	 * @return
	 */
	public abstract Transfer transfer();


	/**
	 * @param transfer
	 * @return this
	 */
	public abstract HttpChannel<IN, OUT> transfer(Transfer transfer);

}
