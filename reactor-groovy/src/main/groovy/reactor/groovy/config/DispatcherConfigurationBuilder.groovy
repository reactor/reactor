package reactor.groovy.config

import groovy.transform.CompileStatic
import reactor.core.config.DispatcherConfiguration
import reactor.core.config.DispatcherType
import reactor.fn.Supplier

/**
 * @author Stephane Maldini
 */
@CompileStatic
class DispatcherConfigurationBuilder implements Supplier<DispatcherConfiguration>{

	final String name

	DispatcherType type = DispatcherType.RING_BUFFER
	Integer backlog
	Integer size

	private DispatcherConfiguration dispatcherConfiguration

	DispatcherConfigurationBuilder(String name) {
		this.name = name
	}

	@Override
	DispatcherConfiguration get() {
		return dispatcherConfiguration ?: new DispatcherConfiguration(name, type, backlog, size)
	}
}
