package reactor.core.configuration

import spock.lang.Specification
import spock.lang.Unroll

class PropertiesConfigurationReaderSpec extends Specification {

	def "Default configuration can be read"() {
		given: "a configuration reader"
		def reader = new PropertiesConfigurationReader()

		when: "the default configuration is read"
		def configuration = reader.read()
		def dispatchers = toMapByName configuration.dispatcherConfigurations

		then: "it contains the expected dispatchers"
		configuration.defaultDispatcherName == 'ringBuffer'
		dispatchers.size() == 3
		matchesExpectedDefaultConfiguration(dispatchers.eventLoop, DispatcherType.EVENT_LOOP, 0, 256)
		matchesExpectedDefaultConfiguration(dispatchers.ringBuffer, DispatcherType.RING_BUFFER, null, 1024)
		matchesExpectedDefaultConfiguration(dispatchers.threadPoolExecutor, DispatcherType.THREAD_POOL_EXECUTOR, 0, 1024)
	}

	def "Custom default configuration can be read"() {
		given: "a configuration reader"
		def reader = new PropertiesConfigurationReader()

		when: "a custom default configuration is read"
		System.setProperty("reactor.profiles.default", "custom")
		def configuration = reader.read()
		System.clearProperty("reactor.profiles.active")

		def dispatchers = toMapByName configuration.dispatcherConfigurations

		then: "it contains the expected dispatchers"
		dispatchers.size() == 1
		matchesExpectedDefaultConfiguration(dispatchers.alpha, DispatcherType.SYNCHRONOUS, null, null)
	}

	def "Additional profiles can be enabled"() {
		given: "a configuration reader"
		def reader = new PropertiesConfigurationReader()

		when: "an additional profile is active"
		System.setProperty("reactor.profiles.active", "custom")
		def configuration = reader.read()
		System.clearProperty("reactor.profiles.active")

		def dispatchers = toMapByName configuration.dispatcherConfigurations

		then: "it contains the expected dispatchers"
		dispatchers.size() == 4
		matchesExpectedDefaultConfiguration(dispatchers.eventLoop, DispatcherType.EVENT_LOOP, 0, 256)
		matchesExpectedDefaultConfiguration(dispatchers.ringBuffer, DispatcherType.RING_BUFFER, null, 1024)
		matchesExpectedDefaultConfiguration(dispatchers.threadPoolExecutor, DispatcherType.THREAD_POOL_EXECUTOR, 0, 1024)
		matchesExpectedDefaultConfiguration(dispatchers.alpha, DispatcherType.SYNCHRONOUS, null, null)
	}

	def "An active profile can override a profile that came before it"() {
		given: "a configuration reader"
		def reader = new PropertiesConfigurationReader()

		when: "two active profiles are enabled"
		System.setProperty("reactor.profiles.active", "custom, override-custom")
		def configuration = reader.read()
		System.clearProperty("reactor.profiles.active")

		def dispatchers = toMapByName configuration.dispatcherConfigurations

		then: "the later profile overrides the earlier profile"
		dispatchers.size() == 4
		matchesExpectedDefaultConfiguration(dispatchers.eventLoop, DispatcherType.EVENT_LOOP, 0, 256)
		matchesExpectedDefaultConfiguration(dispatchers.ringBuffer, DispatcherType.RING_BUFFER, null, 1024)
		matchesExpectedDefaultConfiguration(dispatchers.threadPoolExecutor, DispatcherType.THREAD_POOL_EXECUTOR, 0, 1024)
		matchesExpectedDefaultConfiguration(dispatchers.alpha, DispatcherType.RING_BUFFER, null, null)
	}

	def "A active profile can override the default profile"() {
		given: "a configuration reader"
		def reader = new PropertiesConfigurationReader()

		when: "a profile is enabled"
		System.setProperty("reactor.profiles.active", "override-default")
		def configuration = reader.read()
		System.clearProperty("reactor.profiles.active")

		def dispatchers = toMapByName configuration.dispatcherConfigurations

		then: "the active profile overrides the default profile"
		dispatchers.size() == 3
		matchesExpectedDefaultConfiguration(dispatchers.eventLoop, DispatcherType.EVENT_LOOP, 0, 256)
		matchesExpectedDefaultConfiguration(dispatchers.ringBuffer, DispatcherType.RING_BUFFER, null, 512)
		matchesExpectedDefaultConfiguration(dispatchers.threadPoolExecutor, DispatcherType.THREAD_POOL_EXECUTOR, 0, 1024)
	}

	def "A system property can override existing configuration"() {
		given: "a configuration reader"
		def reader = new PropertiesConfigurationReader()

		when: "a profile is enabled and a system property override is set"
		System.setProperty("reactor.profiles.active", "override-default, custom")
		System.setProperty("reactor.dispatchers.alpha.type", "eventLoop")
		def configuration = reader.read()
		System.clearProperty("reactor.profiles.active")
		System.clearProperty("reactor.dispatchers.alpha.type")

		def dispatchers = toMapByName configuration.dispatcherConfigurations

		then: "the system property takes precedence"
		dispatchers.size() == 4
		matchesExpectedDefaultConfiguration(dispatchers.eventLoop, DispatcherType.EVENT_LOOP, 0, 256)
		matchesExpectedDefaultConfiguration(dispatchers.ringBuffer, DispatcherType.RING_BUFFER, null, 512)
		matchesExpectedDefaultConfiguration(dispatchers.threadPoolExecutor, DispatcherType.THREAD_POOL_EXECUTOR, 0, 1024)
		matchesExpectedDefaultConfiguration(dispatchers.alpha, DispatcherType.EVENT_LOOP, null, null)
	}

	def "Missing active profiles are tolerated"() {
		given: "a configuration reader"
		def reader = new PropertiesConfigurationReader()

		when: "a non-existent profile is enabled"
		System.setProperty("reactor.profiles.active", "does-not-exist")
		def configuration = reader.read()
		System.clearProperty("reactor.profiles.active")

		def dispatchers = toMapByName configuration.dispatcherConfigurations

		then: "its absence is tolerated"
		dispatchers.size() == 3
		matchesExpectedDefaultConfiguration(dispatchers.eventLoop, DispatcherType.EVENT_LOOP, 0, 256)
		matchesExpectedDefaultConfiguration(dispatchers.ringBuffer, DispatcherType.RING_BUFFER, null, 1024)
		matchesExpectedDefaultConfiguration(dispatchers.threadPoolExecutor, DispatcherType.THREAD_POOL_EXECUTOR, 0, 1024)
	}

	def "Missing default profile is tolerated"() {
		given: "a configuration reader"
		def reader = new PropertiesConfigurationReader()

		when: "a non-existent default profile is specified"
		System.setProperty("reactor.profiles.default", "does-not-exist")
		System.setProperty("reactor.dispatchers.default", "alpha")
		def configuration = reader.read()
		System.clearProperty("reactor.profiles.default")
		System.clearProperty("reactor.dispatchers.default")

		def dispatchers = toMapByName configuration.dispatcherConfigurations

		then: "its absence is tolerated"
		dispatchers.size() == 0
	}

	def "Dispatchers with unrecognized type are tolerated"() {
		given: "a configuration reader"
		def reader = new PropertiesConfigurationReader()

		when: "a profile containing an unrecognized dispatcher type is enabled"
		System.setProperty("reactor.profiles.active", "unrecognized-type")
		def configuration = reader.read()
		System.clearProperty("reactor.profiles.active")

		def dispatchers = toMapByName configuration.dispatcherConfigurations

		then: "the unrecognized dispatcher type is tolerated"
		dispatchers.size() == 3
		matchesExpectedDefaultConfiguration(dispatchers.eventLoop, DispatcherType.EVENT_LOOP, 0, 256)
		matchesExpectedDefaultConfiguration(dispatchers.ringBuffer, DispatcherType.RING_BUFFER, null, 1024)
		matchesExpectedDefaultConfiguration(dispatchers.threadPoolExecutor, DispatcherType.THREAD_POOL_EXECUTOR, 0, 1024)
	}

	def cleanup() {
		System.clearProperty('reactor.profiles.default')
	}

	def matchesExpectedDefaultConfiguration(dispatcherConfiguration, type, size, backlog) {
		dispatcherConfiguration.type == type &&
		dispatcherConfiguration.size == size &&
		dispatcherConfiguration.backlog == backlog
	}

	def toMapByName(dispatcherConfigurations) {
		def dispatchersByName = [:];
		dispatcherConfigurations.each { configuration -> dispatchersByName[configuration.name] = configuration }
		dispatchersByName
	}

}
