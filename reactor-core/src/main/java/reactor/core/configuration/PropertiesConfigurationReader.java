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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.IoUtils;

/**
 * A {@link ConfigurationReader} that reads the configuration from properties files
 * and System properties.
 *
 * @author Andy Wilkinson
 */
public class PropertiesConfigurationReader implements ConfigurationReader {

	private static final Pattern REACTOR_NAME_PATTERN = Pattern.compile("reactor\\.dispatchers\\.(.+?)\\.type");

	private static final String FORMAT_DISPATCHER_BACKLOG = "reactor.dispatchers.%s.backlog";
	private static final String FORMAT_DISPATCHER_SIZE    = "reactor.dispatchers.%s.size";
	private static final String FORMAT_DISPATCHER_TYPE    = "reactor.dispatchers.%s.type";
	private static final String FORMAT_RESOURCE_NAME      = "/META-INF/reactor/%s.properties";

	private static final String PROPERTY_PREFIX_REACTOR = "reactor.";

	private static final String PROPERTY_NAME_PROFILES_ACTIVE    = "reactor.profiles.active";
	private static final String PROPERTY_NAME_PROFILES_DEFAULT   = "reactor.profiles.default";
	private static final String PROPERTY_NAME_DEFAULT_DISPATCHER = "reactor.dispatchers.default";

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private final String defaultProfileNameDefault;

	/**
	 * Creates a new {@code PropertiesConfigurationReader} that, by default, will load its
	 * configuration from {@code META-INF/reactor/default.properties}.
	 */
	public PropertiesConfigurationReader() {
		this("default");
	}

	public PropertiesConfigurationReader(String defaultProfileNameDefault) {
		this.defaultProfileNameDefault = defaultProfileNameDefault;
	}

	@Override
	public ReactorConfiguration read() {
		Properties configuration = new Properties();

		applyProfile(loadDefaultProfile(), configuration);

		for(Properties activeProfile : loadActiveProfiles()) {
			applyProfile(activeProfile, configuration);
		}

		applySystemProperties(configuration);

		String defaultDispatcherName = configuration.getProperty(PROPERTY_NAME_DEFAULT_DISPATCHER);

		List<DispatcherConfiguration> dispatcherConfiguration = createDispatcherConfiguration(configuration);

		return new ReactorConfiguration(dispatcherConfiguration, defaultDispatcherName, configuration);
	}

	private Properties loadDefaultProfile() {
		String defaultProfileName = System.getProperty(PROPERTY_NAME_PROFILES_DEFAULT, defaultProfileNameDefault);
		Properties defaultProfile = loadProfile(defaultProfileName);
		return defaultProfile;
	}

	private List<Properties> loadActiveProfiles() {
		List<Properties> activeProfiles = new ArrayList<Properties>();
		if(null != System.getProperty(PROPERTY_NAME_PROFILES_ACTIVE)) {
			String[] profileNames = System.getProperty(PROPERTY_NAME_PROFILES_ACTIVE).split(",");
			for(String profileName : profileNames) {
				activeProfiles.add(loadProfile(profileName.trim()));
			}
		}
		return activeProfiles;
	}

	private void applyProfile(Properties profile, Properties configuration) {
		configuration.putAll(profile);
	}

	private void applySystemProperties(Properties configuration) {
		for(String prop : System.getProperties().stringPropertyNames()) {
			if(prop.startsWith(PROPERTY_PREFIX_REACTOR)) {
				configuration.put(prop, System.getProperty(prop));
			}
		}
	}

	private List<DispatcherConfiguration> createDispatcherConfiguration(Properties configuration) {
		List<String> dispatcherNames = getDispatcherNames(configuration);
		List<DispatcherConfiguration> dispatcherConfigurations = new ArrayList<DispatcherConfiguration>(dispatcherNames.size());
		for(String dispatcherName : dispatcherNames) {
			DispatcherType type = getType(dispatcherName, configuration);
			if(type != null) {
				dispatcherConfigurations.add(new DispatcherConfiguration(dispatcherName,
				                                                         type,
				                                                         getBacklog(dispatcherName,
				                                                                    configuration),
				                                                         getSize(dispatcherName, configuration)));
			}
		}
		return dispatcherConfigurations;
	}

	private List<String> getDispatcherNames(Properties configuration) {
		List<String> dispatcherNames = new ArrayList<String>();

		for(Object propertyName : configuration.keySet()) {
			Matcher matcher = REACTOR_NAME_PATTERN.matcher((String)propertyName);
			if(matcher.matches()) {
				dispatcherNames.add(matcher.group(1));
			}
		}

		return dispatcherNames;
	}

	private DispatcherType getType(String dispatcherName, Properties configuration) {
		String type = configuration.getProperty(String.format(FORMAT_DISPATCHER_TYPE, dispatcherName));
		if("eventLoop".equals(type)) {
			return DispatcherType.EVENT_LOOP;
		} else if("ringBuffer".equals(type)) {
			return DispatcherType.RING_BUFFER;
		} else if("synchronous".equals(type)) {
			return DispatcherType.SYNCHRONOUS;
		} else if("threadPoolExecutor".equals(type)) {
			return DispatcherType.THREAD_POOL_EXECUTOR;
		} else {
			logger.warn("The type '{}' of Dispatcher '{}' is not recognized", type, dispatcherName);
			return null;
		}
	}

	private Integer getBacklog(String dispatcherName, Properties configuration) {
		return getInteger(String.format(FORMAT_DISPATCHER_BACKLOG, dispatcherName), configuration);
	}

	private Integer getSize(String dispatcherName, Properties configuration) {
		return getInteger(String.format(FORMAT_DISPATCHER_SIZE, dispatcherName), configuration);
	}

	private Integer getInteger(String propertyName, Properties configuration) {
		String property = configuration.getProperty(propertyName);
		if(property != null) {
			return Integer.parseInt(property);
		} else {
			return null;
		}
	}

	protected Properties loadProfile(String name) {
		Properties properties = new Properties();
		InputStream inputStream = getClass().getResourceAsStream(String.format(FORMAT_RESOURCE_NAME, name));
		if(null != inputStream) {
			try {
				properties.load(inputStream);
			} catch(IOException e) {
				logger.error("Failed to load properties from '{}' for profile '{}'",
				             String.format(FORMAT_RESOURCE_NAME, name),
				             name,
				             e);
			} finally {
				IoUtils.closeQuietly(inputStream);
			}
		} else {
			logger.debug("No properties file found in the classpath at '{}' for profile '{}'", String.format(
					FORMAT_RESOURCE_NAME,
					name), name);
		}
		return properties;
	}

}
