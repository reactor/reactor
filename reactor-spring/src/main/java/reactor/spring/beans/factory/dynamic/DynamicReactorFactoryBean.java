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

package reactor.spring.beans.factory.dynamic;

import org.springframework.beans.factory.FactoryBean;

import reactor.core.Environment;
import reactor.core.dynamic.DynamicReactor;
import reactor.core.dynamic.DynamicReactorFactory;

/**
 * A Spring {@link FactoryBean} for creating a {@link DynamicReactor} proxy from a given type
 * using a {@link DynamicReactorFactory}.
 *
 * @author Jon Brisbin
 *
 * @param <T> the type for the proxy
 *
 * @see DynamicReactorFactory
 */
public class DynamicReactorFactoryBean<T extends DynamicReactor> implements FactoryBean<T> {

	private final boolean                  singleton;
	private final Class<T>                 type;
	private final DynamicReactorFactory<T> reactorFactory;

  /**
   * Creates a new factory bean that will use a DynamicReactorFactory configured with the
   * given {@code env} and {@code type} to create its bean. The bean will not be a singleton.
   *
   * @param env The environment to use
   * @param type The type to use
   */
	public DynamicReactorFactoryBean(Environment env, Class<T> type) {
		this.singleton = false;
		this.type = type;
		this.reactorFactory = new DynamicReactorFactory<T>(env, type);
	}

	/**
	 * Creates a new factory bean that will use a DynamicReactorFactory configured with the
   * given {@code env} and {@code type} to create its bean. {@code singleton} controls
   * whether or not the factory creates a singleton bean.
   *
	 * @param env The environment to use
	 * @param type The type to use
	 * @param singleton {@code true} if the factory's bean should be singleton, {@code false}
	 *                  otherwise
	 */
	public DynamicReactorFactoryBean(Environment env, Class<T> type, boolean singleton) {
		this.singleton = singleton;
		this.type = type;
		this.reactorFactory = new DynamicReactorFactory<T>(env, type);
	}

	@Override
	public T getObject() throws Exception {
		return reactorFactory.create();
	}

	@Override
	public Class<?> getObjectType() {
		return type;
	}

	@Override
	public boolean isSingleton() {
		return singleton;
	}

}
