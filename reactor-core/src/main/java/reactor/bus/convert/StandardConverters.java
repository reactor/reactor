/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.bus.convert;

import reactor.core.support.Assert;

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

	/**
	 * A {@link DelegatingConverter} that will delegate to a {@link StringToNumberConverter},
	 * a {@link ConstructorParameterConverter} and a {@link ToStringConverter} in that order.
	 */
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

	/**
	 * A {@link Converter} that will convert to the target type by creating a new instance
	 * of the target type using the source, or a conversion of the source, as the argument
	 * that's passed to the target type's constructor.
	 * <p>
	 * For conversion to be possible the target type must have a constructor that requires
	 * a single argument. In the event of the target type having multiple single-argument
	 * constructors, the first single-argument constructor that's found will be used.
	 *
	 * @author Jon Brisbin
	 *
	 */
	public enum ConstructorParameterConverter implements Converter {

		/**
		 * The instance of this converter
		 */
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
				return (T) CTORS.get(targetType).newInstance(source);
			} catch (Throwable t) {
				throw new ConversionFailedException(t, source.getClass(), targetType);
			}
		}
	}

	/**
	 * A {@link Converter} that will convert a {@link String} to a {@link Number}. The
	 * following {@code Number} subclasses are supported:
	 *
	 * <ul>
	 *	<li>{@link AtomicInteger}</li>
	 *	<li>{@link AtomicLong}</li>
	 *	<li>{@link BigInteger}</li>
	 * 	<li>{@link Byte}</li>
	 *	<li>{@link Double}</li>
	 *	<li>{@link Float}</li>
	 * 	<li>{@link Integer}</li>
	 *	<li>{@link Long}</li>
	 *	<li>{@link Short}</li>
	 * </ul>
	 *
	 * @author Jon Brisbin
	 */
	public enum StringToNumberConverter implements Converter {

		/**
		 * The instance of this converter
		 */
		INSTANCE;

		@Override
		public boolean canConvert(Class<?> sourceType, Class<?> targetType) {
			return String.class == sourceType && Number.class.isAssignableFrom(targetType);
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> T convert(Object source, Class<T> targetType) {
			Assert.notNull(source, "Source cannot be null when converting to a Number.");

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

	/**
	 * A {@link Converter} that will convert any Object to a String by
	 * calling its {@code toString} method.
	 *
	 * @author Jon Brisbin
	 *
	 */
	public enum ToStringConverter implements Converter {

		/**
		 * The instance of this converter
		 */
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
