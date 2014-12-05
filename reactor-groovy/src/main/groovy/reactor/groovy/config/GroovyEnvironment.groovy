package reactor.groovy.config

import groovy.transform.CompileStatic
import groovy.transform.TypeCheckingMode
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.customizers.builder.CompilerCustomizationBuilder
import reactor.core.Dispatcher
import reactor.core.Environment
import reactor.event.EventBus

import static groovy.lang.Closure.DELEGATE_FIRST

/**
 * @author Stephane Maldini
 */
@CompileStatic
class GroovyEnvironment {

	private final Map<String, ReactorBuilder> reactors = [:]

	Environment reactorEnvironment

	@CompileStatic(TypeCheckingMode.SKIP)
	static GroovyEnvironment build(Reader r) {
		def configuration = new CompilerConfiguration()
		CompilerCustomizationBuilder.withConfig(configuration) {
			ast(CompileStatic)
		}

		configuration.scriptBaseClass = ReactorScriptWrapper.name
		def shell = new GroovyShell(configuration)
		def script = shell.parse r
		script.run()
	}

	static GroovyEnvironment build(File file) {
		GroovyEnvironment gs = null
		file.withReader { Reader reader ->
			gs = build reader
		}
		gs
	}

	static GroovyEnvironment build(String script) {
		def reader = new StringReader(script)
		GroovyEnvironment gs = build reader
		gs
	}

	/**
	 * Root DSL to a GroovyEnvironment
	 * @param c DSL
	 * @return {@link GroovyEnvironment}
	 */
	static GroovyEnvironment create(@DelegatesTo(strategy = DELEGATE_FIRST, value = GroovyEnvironment) Closure c
	) {
		def configuration = new GroovyEnvironment()

		DSLUtils.delegateFirstAndRun configuration, c

		configuration
	}


	GroovyEnvironment include(GroovyEnvironment... groovyEnvironments) {
		ReactorBuilder reactorBuilder
		ReactorBuilder current
		String key

		for (groovyEnvironment in groovyEnvironments) {

			if (reactorEnvironment) {
				if (groovyEnvironment.reactorEnvironment) {
					for (dispatcherEntry in groovyEnvironment.reactorEnvironment) {
						for (dispatcher in dispatcherEntry.value) {
							reactorEnvironment.addDispatcher(dispatcherEntry.key, dispatcher)
						}
					}
				}
			} else {
				reactorEnvironment = groovyEnvironment.reactorEnvironment
			}

			for (reactorEntry in groovyEnvironment.reactors) {
				reactorBuilder = ((Map.Entry<String, ReactorBuilder>) reactorEntry).value
				key = ((Map.Entry<String, EventBus>) reactorEntry).key

				current = reactors[key]
				if (current) {
					reactorBuilder.rehydrate current
					reactorBuilder.addConsumersFrom current
				} else {
					reactors[key] = reactorBuilder
				}
			}

		}
		this
	}

	/**
	 * initialize a Reactor
	 * @param c DSL
	 */
	ReactorBuilder reactor(@DelegatesTo(strategy = DELEGATE_FIRST, value = ReactorBuilder) Closure c) {
		reactor(null, c)
	}

	ReactorBuilder reactor(String name,
	                       @DelegatesTo(strategy = DELEGATE_FIRST, value = ReactorBuilder) Closure c
	) {
		reactorEnvironment = reactorEnvironment ?: new Environment()

		def builder = new ReactorBuilder(name, reactors)
		builder.init()
		builder.env = reactorEnvironment

		DSLUtils.delegateFirstAndRun builder, c

		builder
	}

	EventBus getAt(String reactor) {
		reactors[reactor]?.get()
	}

	void putAt(String reactorName, EventBus reactor) {
		ReactorBuilder builder = new ReactorBuilder(reactorName, reactors, reactor)
		reactors[reactorName] = builder
	}

	EventBus reactor(String reactor) {
		getAt reactor
	}

	EventBus reactor(String reactorName, EventBus reactor) {
		putAt reactorName, reactor
	}

	Collection<ReactorBuilder> reactorBuildersByExtension(String extensionKey) {
		reactors.findAll { String k, ReactorBuilder v -> v.ext(extensionKey) }.values()
	}

	ReactorBuilder reactorBuilder(String reactor) {
		reactors[reactor]
	}

	ReactorBuilder reactorBuilder(String reactorName, ReactorBuilder reactor) {
		reactors[reactorName] = reactor
	}

	/**
	 * initialize a {@link Environment}
	 * @param c DSL
	 */
	Environment environment(@DelegatesTo(strategy = DELEGATE_FIRST, value = EnvironmentBuilder) Closure c) {
		environment([:], c)
	}

	Environment environment(Map properties,
	                        @DelegatesTo(strategy = DELEGATE_FIRST, value = EnvironmentBuilder) Closure c
	) {
		def builder = new EnvironmentBuilder(properties as Properties)
		DSLUtils.delegateFirstAndRun builder, c
		reactorEnvironment = builder.get()
	}

	Environment environment(Environment environment) {
		this.reactorEnvironment = environment
	}

	Environment environment() {
		this.reactorEnvironment
	}

	Dispatcher dispatcher(String dispatcher) {
		reactorEnvironment?.getDispatcher(dispatcher)
	}

	Dispatcher dispatcher(String dispatcherName, Dispatcher dispatcher) {
		reactorEnvironment?.addDispatcher(dispatcherName, dispatcher)
		dispatcher
	}

}
