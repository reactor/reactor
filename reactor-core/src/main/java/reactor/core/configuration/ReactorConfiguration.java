package reactor.core.configuration;

import java.util.List;
import java.util.Properties;

import reactor.util.Assert;

/**
 * An encapsulation of configuration for Reactor
 *
 * @author Andy Wilkinson
 *
 */
public final class ReactorConfiguration {

	private final List<DispatcherConfiguration> dispatcherConfigurations;

	private final String defaultDispatcherName;

	private final Properties properties;

	ReactorConfiguration(List<DispatcherConfiguration> dispatcherConfigurations, String defaultDispatcherName, Properties properties) {
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
