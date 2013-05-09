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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class DelegatingConverter implements Converter {

	private List<Converter> delegateConverters = Collections.emptyList();

	public DelegatingConverter() {
	}

	public DelegatingConverter(List<Converter> delegateConverters) {
		setDelegateConverters(delegateConverters);
	}

	public static DelegatingConverter create(Converter... converters) {
		List<Converter> delegateConverters = new ArrayList<Converter>(converters.length);
		Collections.addAll(delegateConverters, converters);
		return new DelegatingConverter(delegateConverters);
	}

	public List<Converter> getDelegateConverters() {
		return delegateConverters;
	}

	public DelegatingConverter setDelegateConverters(List<Converter> delegateConverters) {
		if(null == delegateConverters) {
			this.delegateConverters = Collections.emptyList();
		} else {
			this.delegateConverters = delegateConverters;
		}
		return this;
	}

	@Override public boolean canConvert(Class<?> sourceType, Class<?> targetType) {
		if(delegateConverters != null && !delegateConverters.isEmpty()){
			for(Converter c : delegateConverters) {
				if(c.canConvert(sourceType, targetType)) {
					return true;
				}
			}
		}
		return false;
	}

	@Override public <T> T convert(Object source, Class<T> targetType) {
		if(null == source) {
			return null;
		}
		for(Converter c : delegateConverters) {
			if(c.canConvert(source.getClass(), targetType)) {
				return c.convert(source, targetType);
			}
		}
		throw new ConversionFailedException(source.getClass(), targetType);
	}

}
