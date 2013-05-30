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

package reactor.convert;

import reactor.util.Assert;

import java.lang.reflect.Constructor;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jon Brisbin
 */
public abstract class StandardConverters {

	private static final Map<Class<?>, Constructor<?>> CTORS       = Collections.synchronizedMap(new HashMap<Class<?>, Constructor<?>>());
	private static final Map<Class<?>, Class<?>>       CTOR_PARAMS = Collections.synchronizedMap(new HashMap<Class<?>, Class<?>>());

	public static final Converter CONVERTERS = new DelegatingConverter(
			StringToNumberConverter.INSTANCE,
			ConstructorParameterConverter.INSTANCE,
			ToStringConverter.INSTANCE
	);

	protected StandardConverters() {
	}

	private static Class<?> findFirstCtorParam(Class<?> type) {
		Class<?> paramType = CTOR_PARAMS.get(type);
		if (null != paramType) {
			return paramType;
		}

		for (Constructor<?> ctor : type.getConstructors()) {
			if (ctor.getParameterTypes().length == 1) {
				paramType = ctor.getParameterTypes()[0];
				CTOR_PARAMS.put(type, paramType);
				CTORS.put(type, ctor);
				return paramType;
			}
		}

		return null;
	}

	public enum ConstructorParameterConverter implements Converter {
		INSTANCE;

		@Override
		public boolean canConvert(Class<?> sourceType, Class<?> targetType) {
			Class<?> paramType = findFirstCtorParam(targetType);
			return null != paramType && (paramType.isAssignableFrom(sourceType) || CONVERTERS.canConvert(sourceType, paramType));
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> T convert(Object source, Class<T> targetType) {
			Assert.notNull(source, "Source object cannot be null.");

			Class<?> paramType = findFirstCtorParam(targetType);
			if (!paramType.isAssignableFrom(source.getClass())) {
				source = CONVERTERS.convert(source, paramType);
			}

			try {
				return (T) CTORS.get(source.getClass()).newInstance(source);
			} catch (Throwable t) {
				throw new ConversionFailedException(t, source.getClass(), targetType);
			}
		}
	}

	public enum StringToNumberConverter implements Converter {
		INSTANCE;

		@Override
		public boolean canConvert(Class<?> sourceType, Class<?> targetType) {
			return String.class == sourceType && Number.class.isAssignableFrom(targetType);
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> T convert(Object source, Class<T> targetType) {
			Assert.notNull(source, "String cannot be null when converting to a Number.");

			if (Integer.class == targetType) {
				return (T) Integer.valueOf(source.toString());
			} else if (Byte.class == targetType) {
				return (T) Byte.valueOf(source.toString());
			} else if (Long.class == targetType) {
				return (T) Long.valueOf(source.toString());
			} else if (Short.class == targetType) {
				return (T) Short.valueOf(source.toString());
			} else if (Float.class == targetType) {
				return (T) Float.valueOf(source.toString());
			} else if (Double.class == targetType) {
				return (T) Double.valueOf(source.toString());
			} else if (BigInteger.class == targetType) {
				return (T) BigInteger.valueOf(Long.valueOf(source.toString()));
			} else if (AtomicInteger.class == targetType) {
				return (T) new AtomicInteger(Integer.valueOf(source.toString()));
			} else if (AtomicLong.class == targetType) {
				return (T) new AtomicLong(Long.valueOf(source.toString()));
			}

			throw new ConversionFailedException(source.getClass(), targetType);
		}


	}

	public enum ToStringConverter implements Converter {
		INSTANCE;

		@Override
		public boolean canConvert(Class<?> sourceType, Class<?> targetType) {
			return String.class == targetType;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> T convert(Object source, Class<T> targetType) {
			return (T) String.format("%s", source);
		}
	}

}
