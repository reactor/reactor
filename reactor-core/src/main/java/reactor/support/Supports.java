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

package reactor.support;

/**
 * Simple interface that can be applied to components to determine whether or not they support a particular type of
 * use.
 *
 * @param <T> the type of values that may be supported
 *
 * @author Jon Brisbin
 */
public interface Supports<T> {

	/**
	 * Implementations should decided whether they support the given object or not.
	 *
	 * @param obj
	 * 		The object that may or may not be supported.
	 *
	 * @return {@literal true} is this component can deal with the given object, {@literal false}, otherwise.
	 */
	boolean supports(T obj);

}
