package reactor.core.configuration;

/**
 * An encapsulation of the configuration for a {@link Dispatcher}.
 *
 * @author Andy Wilkinson
 *
 */
public class DispatcherConfiguration {

	private final String name;

	private final DispatcherType type;

	private final Integer backlog;

	private final Integer size;

	DispatcherConfiguration(String name, DispatcherType type, Integer backlog, Integer size) {
		this.name = name;
		this.type = type;
		this.backlog = backlog;
		this.size = size;
	}

	/**
	 * Returns the configured size, or {@code null} if the size was not configured
	 *
	 * @return The size
	 */
	public Integer getSize() {
		return size;
	}

	/**
	 * Returns the configured backlog, or {@code null} if the backlog was not configured
	 *
	 * @return The backlog
	 */
	public Integer getBacklog() {
		return backlog;
	}

	/**
	 * Returns the name if the Dispatcher. Never {@code null}.
	 *
	 * @return The name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Returns the type of the Dispatcher. Never {@code null}.
	 *
	 * @return The type
	 */
	public DispatcherType getType() {
		return type;
	}
}
