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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A converter that delegates to one or more converters. The delegates are tried in
 * order until one is found that {@link Converter#canConvert can perform} the
 * conversion.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class DelegatingConverter implements Converter {

	private final List<Converter> delegateConverters;

	/**
	 * Creates a new {@code DelegatingConverter} that will delegate to the
	 * given list of {@code delegateConverters}.
	 *
	 * @param delegateConverters The converters to delegate to
	 */
	public DelegatingConverter(List<Converter> delegateConverters) {
		this.delegateConverters = new ArrayList<Converter>(delegateConverters);
	}

	/**
	 * Creates a new {@code DelegatingConverter} that will delegate to the
	 * given list of {@code delegateConverters}.
	 *
	 * @param delegateConverters The converters to delegate to
	 */
	public DelegatingConverter(Converter... delegateConverters) {
		this.delegateConverters = Arrays.asList(delegateConverters);
	}

	@Override
	public boolean canConvert(Class<?> sourceType, Class<?> targetType) {
		if (delegateConverters != null && !delegateConverters.isEmpty()) {
			for (Converter c : delegateConverters) {
				if (c.canConvert(sourceType, targetType)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public <T> T convert(Object source, Class<T> targetType) {
		if (null == source) {
			return null;
		}
		for (Converter c : delegateConverters) {
			if (c.canConvert(source.getClass(), targetType)) {
				return c.convert(source, targetType);
			}
		}
		throw new ConversionFailedException(source.getClass(), targetType);
	}

}
