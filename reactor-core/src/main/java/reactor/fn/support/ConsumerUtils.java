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

package reactor.fn.support;

import reactor.fn.Consumer;
import reactor.fn.Event;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Jon Brisbin
 */
public abstract class ConsumerUtils {

	private static final ReentrantReadWriteLock           CACHE_LOCK       = new ReentrantReadWriteLock();
	private static final ReentrantReadWriteLock.ReadLock  CACHE_READ_LOCK  = CACHE_LOCK.readLock();
	private static final ReentrantReadWriteLock.WriteLock CACHE_WRITE_LOCK = CACHE_LOCK.writeLock();
	private static final Map<String, Class<?>>            ARG_TYPE_CACHE   = new WeakHashMap<String, Class<?>>();

	private ConsumerUtils() {
	}

	/**
	 * Resolves the type of argument that can be {@link Consumer#accept accepted} by the
	 * given {@code consumer}.
	 *
	 * @param consumer The consumer to examine
	 *
	 * @return The type that can be accepted by the consumer
	 */
	@SuppressWarnings({"unchecked"})
	public static <T> Class<? extends T> resolveArgType(Consumer<?> consumer) {
		Class<? extends T> clazz;
		CACHE_READ_LOCK.lock();
		try {
			clazz = (Class<? extends T>) ARG_TYPE_CACHE.get(consumer.getClass().getName());
			if (null != clazz) {
				return clazz;
			}
		} finally {
			CACHE_READ_LOCK.unlock();
		}

		if (Event.class.isInstance(consumer) && null != ((Event<?>) consumer).getData()) {
			return (Class<? extends T>) ((Event<?>) consumer).getData().getClass();
		}

		for (Type t : consumer.getClass().getGenericInterfaces()) {
			if (t instanceof ParameterizedType) {
				ParameterizedType pt = (ParameterizedType) t;
				Type t1 = pt.getActualTypeArguments()[0];
				if (t1 instanceof ParameterizedType) {
					clazz = (Class<? extends T>) ((ParameterizedType) t1).getRawType();
				} else if (t1 instanceof Class) {
					clazz = (Class<? extends T>) t1;
				}
			}
			if (null != clazz) {
				CACHE_WRITE_LOCK.lock();
				try {
					ARG_TYPE_CACHE.put(consumer.getClass().getName(), clazz);
				} finally {
					CACHE_WRITE_LOCK.unlock();
				}
				break;
			}
		}

		if (null == clazz) {
			for (Method m : consumer.getClass().getDeclaredMethods()) {
				if ("accept".equals(m.getName()) && m.getParameterTypes().length == 1) {
					clazz = (Class<? extends T>) m.getParameterTypes()[0];
					CACHE_WRITE_LOCK.lock();
					try {
						ARG_TYPE_CACHE.put(consumer.getClass().getName(), clazz);
					} finally {
						CACHE_WRITE_LOCK.unlock();
					}
					return clazz;
				}
			}
		} else {
			return clazz;
		}

		return (Class<? extends T>) Object.class;
	}

}
