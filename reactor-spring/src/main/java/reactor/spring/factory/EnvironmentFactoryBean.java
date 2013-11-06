package reactor.spring.factory;

import org.springframework.beans.factory.FactoryBean;
import reactor.core.Environment;
import reactor.core.configuration.ConfigurationReader;
import reactor.core.configuration.PropertiesConfigurationReader;

/**
 * A {@link FactoryBean} implementation that provides easy creation of Reactor a {@link Environment}.
 *
 * @author Jon Brisbin
 */
public class EnvironmentFactoryBean implements FactoryBean<Environment> {

	private final Environment environment;

	/**
	 * Create an {@literal EnvironmentFactoryBean} with the default settings.
	 */
	public EnvironmentFactoryBean() {
		this(new PropertiesConfigurationReader());
	}

	/**
	 * Create an {@literal EnvironmentFactoryBean} using the given profile name as the default Reactor profile.
	 *
	 * @param profile
	 * 		the Reactor profile to use as the default
	 */
	public EnvironmentFactoryBean(String profile) {
		this(new PropertiesConfigurationReader(profile));
	}

	/**
	 * Create an {@literal EnvironmentFactoryBean} using the given {@link ConfigurationReader}.
	 *
	 * @param configReader
	 * 		the {@link ConfigurationReader} to use
	 */
	public EnvironmentFactoryBean(ConfigurationReader configReader) {
		this.environment = new Environment(configReader);
	}

	@Override public Environment getObject() throws Exception {
		return environment;
	}

	@Override public Class<?> getObjectType() {
		return Environment.class;
	}

	@Override public boolean isSingleton() {
		return true;
	}

}

