package reactor.groovy.config

import groovy.transform.CompileStatic
import reactor.Environment
import reactor.core.Dispatcher
import reactor.core.config.ConfigurationReader
import reactor.core.config.DispatcherConfiguration
import reactor.core.config.ReactorConfiguration
import reactor.fn.Supplier

import static groovy.lang.Closure.DELEGATE_FIRST

/**
 * @author Stephane Maldini
 */
@CompileStatic
class EnvironmentBuilder implements ConfigurationReader,Supplier<Environment> {

	private final List<DispatcherConfiguration> dispatcherConfigurations = []
	private final Map<String, Dispatcher> dispatchers = [:]
	private final Properties props
	private Environment environment

	String defaultDispatcher = Environment.SHARED

	EnvironmentBuilder(Properties props) {
		this.props = props
	}

	Environment get(){
		   environment ?: new Environment(dispatchers, this)
	}

	@Override
	ReactorConfiguration read() {
		new ReactorConfiguration(dispatcherConfigurations, defaultDispatcher, props)
	}

	/**
	 * initialize a Dispatcher
	 * @param c DSL
	 * @return {@link Dispatcher}
	 */
	DispatcherConfiguration dispatcher(String name,
			@DelegatesTo(strategy = DELEGATE_FIRST, value = DispatcherConfigurationBuilder) Closure c
	) {
		def builder = new DispatcherConfigurationBuilder(name)
		DSLUtils.delegateFirstAndRun builder, c

		dispatcherConfigurations << builder.get()

		builder.get()
	}

	/**
	 * initialize a Dispatcher
	 * @param c DSL
	 * @return {@link Dispatcher}
	 */
	Dispatcher dispatcher(String name, Dispatcher d) {
		dispatchers[name] = d
		d
	}
}
