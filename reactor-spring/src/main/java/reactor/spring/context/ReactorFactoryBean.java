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

package reactor.spring.context;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.util.Assert;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.ReactorSpec;
import reactor.core.spec.Reactors;

/**
 * @author Jon Brisbin
 */
public class ReactorFactoryBean implements FactoryBean<Reactor> {

	private volatile Reactor     reactor;

	public ReactorFactoryBean(Environment env) {
		this(env, null, null);
	}

	public ReactorFactoryBean(Environment env,
														String dispatcher) {
		this(env, dispatcher, null);
	}

	public ReactorFactoryBean(Environment env,
														String dispatcher,
														EventRouting eventRouting) {
		Assert.notNull(env, "Environment cannot be null.");

		ReactorSpec spec = Reactors.reactor().env(env);
		if (null != dispatcher) {
			if ("sync".equals(dispatcher)) {
				spec.synchronousDispatcher();
			} else {
				spec.dispatcher(dispatcher);
			}
		}
		if (null != eventRouting) {
			switch (eventRouting) {
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
