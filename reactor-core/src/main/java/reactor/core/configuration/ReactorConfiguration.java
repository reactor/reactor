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

package reactor.core.configuration;

import java.util.List;
import java.util.Properties;

import reactor.util.Assert;

/**
 * An encapsulation of configuration for Reactor
 *
 * @author Andy Wilkinson
 * @author Jon Brisbin
 */
public class ReactorConfiguration {

	private final List<DispatcherConfiguration> dispatcherConfigurations;

	private final String defaultDispatcherName;

	private final Properties properties;

	public ReactorConfiguration(List<DispatcherConfiguration> dispatcherConfigurations, String defaultDispatcherName, Properties properties) {
		Assert.notNull(dispatcherConfigurations, "'dispatcherConfigurations' must not be null");
		Assert.notNull(defaultDispatcherName, "'defaultDispatcherName' must not be null");
		Assert.notNull(properties, "'properties' must not be null");

		this.dispatcherConfigurations = dispatcherConfigurations;
		this.defaultDispatcherName = defaultDispatcherName;
		this.properties = properties;
	}

	/**
	 * Returns a {@link List} of {@link DispatcherConfiguration DispatcherConfigurations}. If no
	 * dispatchers are configured, an empty list is returned. Never returns {@code null}.
	 *
	 * @return The dispatcher configurations
	 */
	public List<DispatcherConfiguration> getDispatcherConfigurations() {
		return dispatcherConfigurations;
	}

	/**
	 * Returns the name of the default dispatcher. Never {@code null}.
	 *
	 * @return The default dispatcher's name
	 */
	public String getDefaultDispatcherName() {
		return defaultDispatcherName;
	}

	/**
	 * Additional configuration properties. Never {@code null}.
	 *
	 * @return The additional configuration properties.
	 */
	public Properties getAdditionalProperties() {
		return properties;
	}
}
