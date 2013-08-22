package reactor.groovy.config

import groovy.transform.CompileStatic
import reactor.core.Environment
import reactor.core.Reactor

import static groovy.lang.Closure.*
import groovy.transform.TypeCheckingMode
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.customizers.builder.CompilerCustomizationBuilder

/**
 * @author Stephane Maldini
 */
@CompileStatic
class GroovyEnvironment {

	private final Map<String, Reactor> reactors = [:]

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
	 * Root DSL to findOrCreate a GroovyConfigurationReader System
	 * @param properties
	 * @param c DSL
	 * @return {@link Environment}
	 */
	static GroovyEnvironment create(@DelegatesTo(strategy = DELEGATE_FIRST, value = GroovyEnvironment) Closure c
	) {
		def configuration = new GroovyEnvironment()

		DSLUtils.delegateFirstAndRun configuration, c

		configuration
	}

	/**
	 * initialize a Reactor
	 * @param c DSL
	 */
	Reactor reactor(
			@DelegatesTo(strategy = DELEGATE_FIRST, value = ReactorBuilder) Closure c
	) {
		reactorEnvironment = reactorEnvironment ?: new Environment()

		def builder = new ReactorBuilder(reactors)
		builder.env = reactorEnvironment

		DSLUtils.delegateFirstAndRun builder, c

		builder.get()
	}

	Reactor getAt(String reactor){
		reactors[reactor]
	}

	Reactor reactor(String reactor){
		getAt reactor
	}


	/**
	 * initialize a {@link Environment}
	 * @param c DSL
	 */
	Environment environment(Map properties = [:],
	                        @DelegatesTo(strategy = DELEGATE_FIRST, value = EnvironmentBuilder) Closure c
	) {
		def builder = new EnvironmentBuilder(properties as Properties)
		DSLUtils.delegateFirstAndRun builder, c
		reactorEnvironment = builder.get()
	}

	Environment environment(Environment environment){
		this.reactorEnvironment = environment
	}

	Environment environment(){
		this.reactorEnvironment
	}

}
