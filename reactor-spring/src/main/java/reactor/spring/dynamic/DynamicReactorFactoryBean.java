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

package reactor.spring.dynamic;

import org.springframework.beans.factory.FactoryBean;
import reactor.core.dynamic.DynamicReactor;
import reactor.core.dynamic.DynamicReactorFactory;

/**
 * @author Jon Brisbin
 */
public class DynamicReactorFactoryBean<T extends DynamicReactor> implements FactoryBean<T> {

	private final boolean                  singleton;
	private final Class<T>                 type;
	private final DynamicReactorFactory<T> reactorFactory;

	public DynamicReactorFactoryBean(Class<T> type) {
		this.singleton = false;
		this.type = type;
		this.reactorFactory = new DynamicReactorFactory<T>(type);
	}

	public DynamicReactorFactoryBean(Class<T> type, boolean singleton) {
		this.singleton = singleton;
		this.type = type;
		this.reactorFactory = new DynamicReactorFactory<T>(type);
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
