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

/**
 * @author Stephane Maldini (smaldini)
 * @date: 3/21/13
 * <p/>
 * Port when Spring org.springframework.core.convert.ConversionService
 */
public interface Converter {

	/**
	 * Returns true if objects of sourceType can be converted to targetType. If this method returns true, it means {@link
	 * #convert(Object, Class)} is capable of converting an instance of sourceType to targetType.
	 *
	 * @param sourceType the source type to convert when (may be null if source is null)
	 * @param targetType the target type to convert to (required)
	 * @return true if a conversion can be performed, false if not
	 */
	boolean canConvert(Class<?> sourceType, Class<?> targetType);

	/**
	 * Convert the source to targetType.
	 *
	 * @param source     the source object to convert (may be null)
	 * @param targetType the target type to convert to (required)
	 * @return the converted object, an instance of targetType
	 */
	<T> T convert(Object source, Class<T> targetType);

}
