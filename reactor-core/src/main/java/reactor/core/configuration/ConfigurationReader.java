package reactor.core.configuration;


/**
 * A {@code ConfigurationReader} is used to read Reactor configuration.
 *
 * @author Andy Wilkinson
 *
 */
public interface ConfigurationReader {

	/**
	 * Reads the configuration
	 *
	 * @return The configuration. Never {@code null}.
	 */
	ReactorConfiguration read();
}
