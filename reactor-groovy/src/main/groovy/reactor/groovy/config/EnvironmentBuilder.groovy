package reactor.groovy.config

import groovy.transform.CompileStatic
import reactor.core.Environment
import reactor.core.configuration.ConfigurationReader
import reactor.core.configuration.DispatcherConfiguration
import reactor.core.configuration.ReactorConfiguration
import reactor.event.dispatch.Dispatcher
import reactor.function.Supplier

import static groovy.lang.Closure.*
/**
 * @author Stephane Maldini
 */
@CompileStatic
class EnvironmentBuilder implements ConfigurationReader,Supplier<Environment> {

	private final List<DispatcherConfiguration> dispatcherConfigurations = []
	private final Map<String, List<Dispatcher>> dispatchers = [:]
	private final Properties props
	private Environment environment

	String defaultDispatcher = Environment.RING_BUFFER

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
		def list = dispatchers[name]
		if (!list) {
			dispatchers[name] = list = []
		}

		list << d
		d
	}
}
