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
 *
 * A Request/Response {@link ChannelStream} extension that provides for several helpers to control HTTP behavior and
 *  observe its metadata.
 *
 * @author Sebastien Deleuze
 * @author Stephane maldini
 */
public abstract class HttpChannel<IN, OUT> extends ChannelStream<IN, OUT> {

	public static final String WS_SCHEME = "ws";
	public static final String WSS_SCHEME = "wss";
	public static final String HTTP_SCHEME = "http";
	public static final String HTTPS_SCHEME = "https";

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
	 * @return a map of resolved parameters against their matching key name
	 */
	public final Map<String, String> params() {
		return null != paramsResolver ? paramsResolver.resolve(uri()) : null;
	}

	/**
	 * Read URI param from the given key
	 *
	 * @param key matching key
	 *
	 * @return the resolved parameter for the given key name
	 */
	public final String param(String key) {
		Map<String, String> params = null;
		if (paramsResolver != null) {
			params = this.paramsResolver.resolve(uri());
		}
		return null != params ? params.get(key) : null;
	}

	/**
	 * @return Resolved HTTP request headers
	 */
	public abstract HttpHeaders headers();

	/**
	 * Register an HTTP request header
	 *
	 * @param name Header name
	 * @param value Header content
	 *
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
	 * Accumulate a Request Header using the given name and value, appending ";" for each new value
	 *
	 * @param name
	 * @param value
	 *
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
	 * @return the resolved request protocol (HTTP 1.1 etc)
	 */
	public abstract Protocol protocol();

	/**
	 * @return the resolved target address
	 */
	public abstract String uri();

	/**
	 * @return the resolved request method (HTTP 1.1 etc)
	 */
	public abstract Method method();


	void paramsResolver(HeaderResolver<String> headerResolver) {
		this.paramsResolver = headerResolver;
	}

	// RESPONSE contract

	/**
	 * @return the resolved HTTP Response Status
	 */
	public abstract Status responseStatus();

	/**
	 * Set the response status to an outgoing response
	 *
	 * @param status the status to define
	 *
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
	 * @return the resolved response HTTP headers
	 */
	public abstract ResponseHeaders responseHeaders();

	/**
	 * Define the response HTTP header for the given key
	 *
	 * @param name the HTTP response header key to override
	 * @param value the HTTP response header content
	 *
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
	 * Accumulate a response HTTP header for the given key name, appending ";" for each new value
	 *
	 * @param name the HTTP response header name
	 * @param value the HTTP response header value
	 *
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
	 * @return the Transfer setting for this http connection (e.g. event-stream)
	 */
	public abstract Transfer transfer();


	/**
	 * Define the Transfer mode for this http connection
	 *
	 * @param transfer the new transfer mode
	 *
	 * @return this
	 */
	public abstract HttpChannel<IN, OUT> transfer(Transfer transfer);

}
