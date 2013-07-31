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

package reactor.spring.beans.factory;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.util.Assert;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.ReactorSpec;
import reactor.core.spec.Reactors;

/**
 * A Spring {@link FactoryBean} for creating a singleton {@link Reactor}.
 *
 * @author Jon Brisbin
 */
public class ReactorFactoryBean implements FactoryBean<Reactor> {

	private volatile Reactor reactor;

	/**
	 * Creates a new ReactorFactoryBean that will use the given environment when configuring
	 * and creating its Reactor. The Reactor will use a synchronous dispatcher and broadcast
	 * event routing.
	 *
	 * @param env
	 * 		The environment to use
	 */
	public ReactorFactoryBean(Environment env) {
		this(env, null, null);
	}

	/**
	 * Creates a new ReactorFactoryBean that will use the given environment when configuring
	 * and creating its Reactor and will configure the reactor with the dispatcher with
	 * the given name found in the environment. The Reactor will use broadcast event routing.
	 *
	 * @param env
	 * 		The environment to use
	 * @param dispatcher
	 * 		The dispatcher to configure the Reactor with
	 */
	public ReactorFactoryBean(Environment env,
	                          String dispatcher) {
		this(env, dispatcher, null);
	}

	/**
	 * Creates a new ReactorFactoryBean that will use the given environment when configuring
	 * and creating its Reactor and will configure the reactor with the dispatcher with
	 * the given name found in the environment. The Reactor will use the given
	 * {@code eventRouting}.
	 *
	 * @param env
	 * 		The environment to use
	 * @param dispatcher
	 * 		The dispatcher to configure the Reactor with
	 * @param eventRouting
	 * 		The type of event routing to use
	 */
	public ReactorFactoryBean(Environment env,
	                          String dispatcher,
	                          EventRouting eventRouting) {
		Assert.notNull(env, "Environment cannot be null.");

		ReactorSpec spec = Reactors.reactor().env(env);
		if(null != dispatcher) {
			if("sync".equals(dispatcher)) {
				spec.synchronousDispatcher();
			} else {
				spec.dispatcher(dispatcher);
			}
		}
		if(null != eventRouting) {
			switch(eventRouting) {
				case BROADCAST_EVENT_ROUTING:
					spec.broadcastEventRouting();
					break;
				case RANDOM_EVENT_ROUTING:
					spec.randomEventRouting();
					break;
				case ROUND_ROBIN_EVENT_ROUTING:
					spec.roundRobinEventRouting();
					break;
			}
		}
		this.reactor = spec.get();
	}

	@Override
	public Reactor getObject() throws Exception {
		return reactor;
	}

	@Override
	public Class<?> getObjectType() {
		return Reactor.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

}
