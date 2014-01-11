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

package reactor.event;

import reactor.core.ObjectPool;
import reactor.core.Poolable;
import reactor.function.Consumer;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;
import reactor.util.Assert;
import reactor.util.UUIDUtils;

import java.io.Serializable;
import java.util.*;

/**
 * Wrapper for an object that needs to be processed by {@link reactor.function.Consumer}s.
 *
 * @param <T>
 * 		The type of the wrapped object
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Event<T> implements Serializable, Poolable {

	private static final long serialVersionUID = -2476263092040373361L;

  private volatile int     poolPosition;
	private volatile UUID    id;
  private volatile Headers headers;
	private volatile Object  replyTo;
	private volatile Object  key;
	private volatile T       data;

	private transient Consumer<Throwable> errorConsumer;

  protected Event(Class<T> eventType, Headers headers,
                  T data, Consumer<Throwable> errorConsumer, int poolPosition) {

  }
	/**
	 * Creates a new Event with the given {@code headers}, {@code data} and
	 * {@link reactor.function.Consumer<java.lang.Throwable>}.
	 *
	 * @param headers
	 * 		The headers
	 * @param data
	 * 		The data
	 * @param errorConsumer
	 * 		error consumer callback
	 */
	protected Event(Headers headers, T data, Consumer<Throwable> errorConsumer, int poolPosition) {
		this.headers = headers;
		this.data = data;
		this.errorConsumer = errorConsumer;
    this.poolPosition = poolPosition;
	}

  public static <T> Event<T> lease(T obj) {
    Class t = (obj != null) ? obj.getClass() : Void.class;
    if (!eventPools.containsKey(t)) {
      eventPools.put(t, new EventPool<T>(ObjectPool.DEFAULT_INITIAL_POOL_SIZE));
    }

    EventPool<T> ep = eventPools.get(t);
    return ep.allocate();
  }

  /**
	 * Wrap the given object with an {@link Event}.
	 *
	 * @param obj
	 * 		The object to wrap.
	 *
	 * @return The new {@link Event}.
	 */
	public static <T> Event<T> wrap(T obj) {
    Event<T> e = lease(obj);
    e.setData(obj);
    return e;
  }

	/**
	 * Wrap the given object with an {@link Event} and set the {@link Event#getReplyTo() replyTo} to the given {@code
	 * replyToKey}.
	 *
	 * @param obj
	 * 		The object to wrap.
	 * @param replyToKey
	 * 		The key to use as a {@literal replyTo}.
	 * @param <T>
	 * 		The type of the given object.
	 *
	 * @return The new {@link Event}.
	 */
	public static <T> Event<T> wrap(T obj, Object replyToKey) {
    Event<T> e = lease(obj);
    e.setData(obj);
    e.setReplyTo(replyToKey);
    return e;
  }

  public static <T> Event<T> wrap(T obj, Object replyToKey,
                                  Headers headers, Consumer<Throwable> errorConsumer) {
    Event<T> e = lease(obj);
    e.setData(obj);
    e.replyTo = replyToKey;
    e.headers = headers;
    e.errorConsumer = errorConsumer;
    return e;
  }

	/**
	 * Get the globally-unique id of this event.
	 *
	 * @return Unique {@link UUID} of this event.
	 */
	public synchronized UUID getId() {
		if(null == id) {
			id = UUIDUtils.create();
		}
		return id;
	}

	/**
	 * Get the {@link Headers} attached to this event.
	 *
	 * @return The Event's Headers
	 */
	public synchronized Headers getHeaders() {
		if(null == headers) {
      headers = new Headers();
		}
		return headers;
	}

	/**
	 * Get the key to send replies to.
	 *
	 * @return The reply-to key
	 */
	public Object getReplyTo() {
		return replyTo;
	}

	/**
	 * Set the {@code key} that interested parties should send replies to.
	 *
	 * @param replyTo
	 * 		The key to use to notify sender of replies.
	 *
	 * @return {@literal this}
	 */
	public Event<T> setReplyTo(Object replyTo) {
		Assert.notNull(replyTo, "ReplyTo cannot be null.");
		this.replyTo = replyTo;
		return this;
	}

	/**
	 * Get the key this event was notified on.
	 *
	 * @return The key used to notify consumers of this event.
	 */
	public Object getKey() {
		return key;
	}

	/**
	 * Set the key this event is being notified with.
	 *
	 * @param key
	 * 		The key used to notify consumers of this event.
	 *
	 * @return {@literal this}
	 */
	public Event<T> setKey(Object key) {
		this.key = key;
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
	 * @param data
	 * 		Data to wrap.
	 *
	 * @return {@literal this}
	 */
	public Event<T> setData(T data) {
		this.data = data;
		return this;
	}

	/**
	 * Get the internal error consumer callback being wrapped.
	 *
	 * @return the consumer.
	 */
	public Consumer<Throwable> getErrorConsumer() {
		return errorConsumer;
	}

	/**
	 * Create a copy of this event, reusing same headers, data and replyTo
	 *
	 * @return {@literal event copy}
	 */
	public Event<T> copy() {
		return copy(data);
	}

	/**
	 * Create a copy of this event, reusing same headers and replyTo
	 *
	 * @return {@literal event copy}
	 */
	public <T> Event<T> copy(T data) {
    Event<T> e = lease(data);

    e.setData(data);
    e.headers = headers;
    e.errorConsumer = errorConsumer;

		if(null != replyTo) {
      e.setReplyTo(replyTo);
    }

    return e;
	}

	/**
	 * Consumes error, using a producer defined callback
	 *
	 * @param throwable
	 * 		The error to consume
	 */
	public void consumeError(Throwable throwable) {
		if(null != errorConsumer) {
			errorConsumer.accept(throwable);
		}
	}


	@Override
	public String toString() {
		return "Event{" +
				"id=" + id +
				", headers=" + headers +
				", replyTo=" + replyTo +
				", data=" + data +
				'}';
	}

  @Override
  public void free() {
    Class t = (data != null) ? data.getClass() : Void.class;
    this.data = null;
    this.headers = null;
    this.replyTo = null;
    this.key = null;
    eventPools.get(t).deallocate(poolPosition);
  }

  /**
	 * Headers are a Map-like structure of name-value pairs. Header names are case-insensitive,
	 * as determined by {@link String#CASE_INSENSITIVE_ORDER}. A header can be removed by
	 * setting its value to {@code null}.
	 */
	public static class Headers implements Serializable, Iterable<Tuple2<String, String>> {

		/**
		 * The name of the origin header
		 *
		 * @see #setOrigin(String)
		 * @see #setOrigin(UUID)
		 * @see #getOrigin()
		 */
		public static final String ORIGIN = "x-reactor-origin";

		private static final long serialVersionUID = 4984692586458514948L;

		private final Object monitor = UUIDUtils.create();
		private final Map<String, String> headers;

		private Headers(boolean sealed, Map<String, String> headers) {
			Map<String, String> copy = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
			copyHeaders(headers, copy);
			if(sealed) {
				this.headers = Collections.unmodifiableMap(copy);
			} else {
				this.headers = copy;
			}
		}

		/**
		 * Creates a new Headers instance by copying the contents of the given {@code headers} Map.
		 * Note that, as the map is copied, subsequent changes to its contents will have no
		 * effect upon the Headers.
		 *
		 * @param headers
		 * 		The map to copy.
		 */
		public Headers(Map<String, String> headers) {
			this(false, headers);
		}

		/**
		 * Create an empty Headers
		 */
		public Headers() {
			this(false, null);
		}

		/**
		 * Sets all of the headers represented by entries in the given {@code headers} Map.
		 * Any entry with a null value will cause the header matching the entry's name to
		 * be removed.
		 *
		 * @param headers
		 * 		The map of headers to set.
		 *
		 * @return {@code this}
		 */
		public Headers setAll(Map<String, String> headers) {
			if(null == headers || headers.isEmpty()) {
				return this;
			} else {
				synchronized(this.monitor) {
					copyHeaders(headers, this.headers);
				}
			}
			return this;
		}

		/**
		 * Set the header value. If {@code value} is {@code null} the header with the given {@code
		 * name} will be removed.
		 *
		 * @param name
		 * 		The name of the header.
		 * @param value
		 * 		The header's value.
		 *
		 * @return {@code this}
		 */
		public Headers set(String name, String value) {
			synchronized(this.monitor) {
				setHeader(name, value, headers);
			}
			return this;
		}

		/**
		 * Set the origin header. The origin is simply a unique id to indicate to consumers where
		 * it should send replies. If {@code id} is {@code null} the origin header will be removed.
		 *
		 * @param id
		 * 		The id of the origin component.
		 *
		 * @return {@code this}
		 */
		public Headers setOrigin(UUID id) {
			String idString = id == null ? null : id.toString();
			return setOrigin(idString);
		}

		/**
		 * Set the origin header. The origin is simply a unique id to indicate to consumers where
		 * it should send replies. If {@code id} is {@code null} this origin header will be removed.
		 *
		 * @param id
		 * 		The id of the origin component.
		 *
		 * @return {@code this}
		 */
		public Headers setOrigin(String id) {
			synchronized(this.monitor) {
				setHeader(ORIGIN, id, headers);
			}
			return this;
		}

		/**
		 * Get the origin header
		 *
		 * @return The origin header, may be {@code null}.
		 */
		public String getOrigin() {
			synchronized(this.monitor) {
				return headers.get(ORIGIN);
			}
		}

		/**
		 * Get the value for the given header.
		 *
		 * @param name
		 * 		The header name.
		 *
		 * @return The value of the header, or {@code null} if none exists.
		 */
		public String get(String name) {
			synchronized(monitor) {
				return headers.get(name);
			}
		}

		/**
		 * Determine whether the headers contain a value for the given name.
		 *
		 * @param name
		 * 		The header name.
		 *
		 * @return {@code true} if a value exists, {@code false} otherwise.
		 */
		public boolean contains(String name) {
			synchronized(monitor) {
				return headers.containsKey(name);
			}
		}

		/**
		 * Get these headers as an unmodifiable {@link Map}.
		 *
		 * @return The unmodifiable header map
		 */
		public Map<String, String> asMap() {
			synchronized(monitor) {
				return Collections.unmodifiableMap(headers);
			}
		}

		/**
		 * Get the headers as a read-only version
		 *
		 * @return A read-only version of the headers.
		 */
		public Headers readOnly() {
			synchronized(monitor) {
				return new Headers(true, headers);
			}
		}

		/**
		 * Returns an unmodifiable Iterator over a copy of this Headers' contents.
		 */
		@Override
		public Iterator<Tuple2<String, String>> iterator() {
			synchronized(this.monitor) {
				List<Tuple2<String, String>> headers = new ArrayList<Tuple2<String, String>>(this.headers.size());
				for(Map.Entry<String, String> header : this.headers.entrySet()) {
					headers.add(Tuple.of(header.getKey(), header.getValue()));
				}
				return Collections.unmodifiableList(headers).iterator();
			}
		}

		private void copyHeaders(Map<String, String> source, Map<String, String> target) {
			if(source != null) {
				for(Map.Entry<String, String> entry : source.entrySet()) {
					setHeader(entry.getKey(), entry.getValue(), target);
				}
			}
		}

		private void setHeader(String name, String value, Map<String, String> target) {
			if(value == null) {
				target.remove(name);
			} else {
				target.put(name, value);
			}
		}
	}

  public static HashMap<Class, EventPool> eventPools = new HashMap<Class, EventPool>();

  public static class EventPool<T> extends ObjectPool<Event<T>> {

    int allocatedSoFar = 0;
    public EventPool(int prealloc) {
      super(prealloc);
    }

    @Override
    public Event<T> newInstance(int poolPosition) {
      allocatedSoFar++;
      return new Event<T>(null, null, null, poolPosition);
    }
  }

}
