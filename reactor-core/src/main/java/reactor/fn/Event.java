/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.fn;

import com.eaio.uuid.UUID;
import reactor.util.Assert;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wrapper for an object that needs to be processed by {@link Consumer}s.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Event<T> {

	private final UUID id = new UUID();
	private Headers headers;
	private Object  replyTo;
	private T       data;

	public Event(Headers headers, T data) {
		this.headers = headers;
		this.data = data;
	}

	public Event(T data) {
		this.data = data;
	}

	/**
	 * Get the globally-unique id of this event.
	 *
	 * @return Unique {@link UUID} of this event.
	 */
	public UUID getId() {
		return id;
	}

	/**
	 * Get the {@link Headers} attached to this event.
	 *
	 * @return
	 */
	public Headers getHeaders() {
		if (null == headers) {
			headers = new Headers();
		}
		return headers;
	}

	/**
	 * Get the key to send replies to.
	 *
	 * @return
	 */
	public Object getReplyTo() {
		return replyTo;
	}

	/**
	 * Set the {@code key} that interested parties should send replies to.
	 *
	 * @param replyTo The key to use to notify sender of replies.
	 * @return {@literal this}
	 */
	public Event<T> setReplyTo(Object replyTo) {
		Assert.notNull(replyTo, "ReplyTo cannot be null.");
		this.replyTo = replyTo;
		return this;
	}

	/**
	 * Get the internal data being wrapped.
	 *
	 * @return The data.
	 */
	public T getData() {
		return data;
	}

	/**
	 * Set the internal data to wrap.
	 *
	 * @param data Data to wrap.
	 * @return {@literal this}
	 */
	public Event<T> setData(T data) {
		this.data = data;
		return this;
	}

	/**
	 * Headers are backed by a {@code Map&lt;String, String&gt;} and provide a little extra sugar for creating read-only
	 * versions and the like.
	 */
	public static class Headers implements Serializable, Iterable<Map.Entry<String, String>> {
		public static final  String ORIGIN           = "x-reactor-origin";
		private static final long   serialVersionUID = 4984692586458514948L;
		private final Map<String, String> headers;

		private Headers(boolean sealed, Map<String, String> headers) {
			if (sealed) {
				this.headers = Collections.unmodifiableMap(headers);
			} else {
				this.headers = headers;
			}
		}

		/**
		 * Create headers using the existing {@link Map}.
		 *
		 * @param headers The map to use as the headers.
		 */
		public Headers(Map<String, String> headers) {
			this(false, headers);
		}

		/**
		 * Create headers using a new, empty map.
		 */
		public Headers() {
			this(false, new ConcurrentHashMap<String, String>());
		}

		/**
		 * Set all headers from the given {@link Map}.
		 *
		 * @param headers The map to use as the headers.
		 * @return
		 */
		public Headers setAll(Map<String, String> headers) {
			if (null == headers || headers.isEmpty()) {
				return this;
			}
			this.headers.putAll(headers);
			return this;
		}

		/**
		 * Set the header value.
		 *
		 * @param name  The name of the header.
		 * @param value The header's value.
		 * @return {@literal this}
		 */
		public Headers set(String name, String value) {
			headers.put(name.toLowerCase(), value);
			return this;
		}

		/**
		 * Set the origin of this event. The origin is simply a unique id to indicate to consumers where it should send
		 * replies.
		 *
		 * @param id The id of the origin component.
		 * @return {@literal this}
		 */
		public Headers setOrigin(UUID id) {
			return setOrigin(id.toString());
		}

		/**
		 * Set the origin of this event. The origin is simply a unique id to indicate to consumers where it should send
		 * replies.
		 *
		 * @param id The id of the origin component.
		 * @return {@literal this}
		 */
		public Headers setOrigin(String id) {
			headers.put(ORIGIN, id);
			return this;
		}

		/**
		 * Get the id of the origin of this event.
		 *
		 * @return The unique id of the component in which this event originated.
		 */
		public String getOrigin() {
			return headers.get(ORIGIN);
		}

		/**
		 * Get the value for the given header.
		 *
		 * @param name The header name.
		 * @return The value of the header, or {@literal null} if none exists.
		 */
		public String get(String name) {
			return headers.get(name.toLowerCase());
		}

		/**
		 * Determine whether the headers contain a value for the given name.
		 *
		 * @param name The header name.
		 * @return {@literal true} if a value exists, {@literal false} otherwise.
		 */
		public boolean contains(String name) {
			return headers.containsKey(name.toLowerCase());
		}

		/**
		 * Get these headers as a {@link Map}.
		 *
		 * @return The headers as a map.
		 */
		public Map<String, String> asMap() {
			return Collections.unmodifiableMap(headers);
		}

		/**
		 * Get the headers as a read-only version. No other values can be added.
		 *
		 * @return A read-only version of the headers.
		 */
		public Headers readOnly() {
			return new Headers(true, headers);
		}

		@Override
		public Iterator<Map.Entry<String, String>> iterator() {
			return headers.entrySet().iterator();
		}
	}

}
