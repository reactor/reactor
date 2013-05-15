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

package reactor.spring.context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionService;

import reactor.convert.Converter;
import reactor.core.R;
import reactor.core.Reactor;
import reactor.fn.dispatch.Dispatcher;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class ReactorFactoryBean implements FactoryBean<Reactor> {

	private static final Reactor ROOT_REACTOR = new Reactor();

	private final Logger log = LoggerFactory.getLogger(getClass());

	static {
		R.link(ROOT_REACTOR);
	}

	@Autowired(required = false)
	private ConversionService conversionService;
	private boolean rootReactor = false;
	private String     name;
	private Dispatcher dispatcher;

	public ReactorFactoryBean(boolean rootReactor) {
		this.rootReactor = rootReactor;
	}

	public ReactorFactoryBean() {
	}

	public ConversionService getConversionService() {
		return conversionService;
	}

	public ReactorFactoryBean setConversionService(ConversionService conversionService) {
		this.conversionService = conversionService;
		return this;
	}

	public boolean isRootReactor() {
		return rootReactor;
	}

	public ReactorFactoryBean setRootReactor(boolean rootReactor) {
		this.rootReactor = rootReactor;
		return this;
	}

	public String getName() {
		return name;
	}

	public ReactorFactoryBean setName(String name) {
		this.name = name;
		return this;
	}

	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	public ReactorFactoryBean setDispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		return this;
	}

	@Override
	public Reactor getObject() throws Exception {
		if (rootReactor) {
			warnIfReactorShouldBeCustomized();
			return ROOT_REACTOR;
		} else if (null != name) {
			Reactor reactor = R.get(name);
			if (null != reactor) {
				warnIfReactorShouldBeCustomized();
				return reactor;
			}
		}

		Converter converter = null;
		if (conversionService != null) {
			converter = new ConversionServiceConverter(conversionService);
		}

		return new Reactor(dispatcher, null, null, converter);
	}

	@Override
	public Class<?> getObjectType() {
		return Reactor.class;
	}

	@Override
	public boolean isSingleton() {
		return rootReactor || null != name;
	}

	private void warnIfReactorShouldBeCustomized() {
		if (dispatcher != null) {
			log.warn("Dispatcher cannot be customized as an existing Reactor is being reused");
		}
		if (conversionService != null) {
			log.warn("Converter cannot be customized as an existing Reactor is being reused");
		}
	}

}
