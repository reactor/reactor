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

package reactor.core.dynamic.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.dynamic.DynamicReactorFactory;

/**
 * Used on a {@class DynamicReactor} to specify the {@link Dispatcher} that should be used
 * by the underlying {@link Reactor}. The {@code Dispatcher} is looked up in the
 * {@link Environment} of the {@link DynamicReactorFactory}.
 *
 * @author Jon Brisbin
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Dispatcher {

	/**
	 * The name of the dispatcher. The actual dispatcher is found by using the name to {@link
	 * Environment#getDispatcher(String) look it up} in the {@link Environment}.
	 *
	 * @return The name of the dispatcher
	 */
	String value();
}
