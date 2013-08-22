package reactor.groovy.config

import groovy.transform.CompileStatic
import reactor.core.configuration.DispatcherConfiguration
import reactor.core.configuration.DispatcherType
import reactor.function.Supplier

/**
 * @author Stephane Maldini
 */
@CompileStatic
class DispatcherConfigurationBuilder implements Supplier<DispatcherConfiguration>{

	String name
	DispatcherType type = DispatcherType.RING_BUFFER
	Integer backlog
	Integer size

	private DispatcherConfiguration dispatcherConfiguration

	@Override
	DispatcherConfiguration get() {
		return dispatcherConfiguration ?: new DispatcherConfiguration(name, type, backlog, size)
	}
}
