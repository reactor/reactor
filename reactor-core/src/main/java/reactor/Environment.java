/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.bus.EventBus;
import reactor.bus.convert.StandardConverters;
import reactor.bus.filter.Filter;
import reactor.bus.filter.RoundRobinFilter;
import reactor.core.Dispatcher;
import reactor.core.DispatcherSupplier;
import reactor.core.config.*;
import reactor.core.dispatch.*;
import reactor.core.dispatch.wait.AgileWaitingStrategy;
import reactor.core.internal.PlatformDependent;
import reactor.core.support.LinkedMultiValueMap;
import reactor.core.support.MultiValueMap;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.fn.timer.HashWheelTimer;
import reactor.fn.timer.Timer;
import reactor.jarjar.com.lmax.disruptor.WaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Environment implements Iterable<Map.Entry<String, List<Dispatcher>>>, Closeable {


	/**
	 * The name of the default ring buffer group dispatcher
	 */
	public static final String DISPATCHER_GROUP = "dispatcherGroup";

	/**
	 * The name of the default shared dispatcher
	 */
	public static final String SHARED = "shared";

	/**
	 * The name of the default mpsc dispatcher
	 */
	public static final String MPSC = "mpsc";

	/**
	 * The name of the default thread pool dispatcher
	 */
	public static final String THREAD_POOL = "threadPoolExecutor";

	/**
	 * The name of the default work queue dispatcher
	 */
	public static final String WORK_QUEUE = "workQueue";

	/**
	 * The number of processors available to the runtime
	 *
	 * @see Runtime#availableProcessors()
	 */
	public static final int PROCESSORS = Runtime.getRuntime().availableProcessors() > 1 ?
			Runtime.getRuntime().availableProcessors() :
			2;

	// GLOBAL
	private static final AtomicReference<Environment> enviromentReference = new AtomicReference<>();

	/**
	 * Create and assign a context environment bound to the current classloader.
	 *
	 * @return the produced {@link Environment}
	 */
	public static Environment initialize() {
		return assign(new Environment());
	}

	/**
	 * Create and assign a context environment bound to the current classloader only if it not already set. Otherwise
	 * returns
	 * the current context environment
	 *
	 * @return the produced {@link Environment}
	 */
	public static Environment initializeIfEmpty() {
		if (alive()) {
			return get();
		} else {
			return assign(new Environment());
		}
	}

	/**
	 * Assign an environment to the context in order to make it available statically in the application from the current
	 * classloader.
	 *
	 * @param environment The environment to assign to the current context
	 * @return the assigned {@link Environment}
	 */
	public static Environment assign(Environment environment) {
		if (!enviromentReference.compareAndSet(null, environment)) {
			environment.shutdown();
			throw new IllegalStateException("An environment is already initialized in the current context");
		}
		return environment;
	}

	/**
	 * Read if the context environment has been set
	 *
	 * @return true if context environment is initialized
	 */
	public static boolean alive() {
		return enviromentReference.get() != null;
	}

	/**
	 * Read the context environment. It must have been previously assigned with
	 * {@link this#assign(Environment)}.
	 *
	 * @return the context environment.
	 * @throws java.lang.IllegalStateException if there is no environment initialized.
	 */
	public static Environment get() throws IllegalStateException {
		Environment environment = enviromentReference.get();
		if (environment == null) {
			throw new IllegalStateException("The environment has not been initialized yet");
		}
		return environment;
	}

	/**
	 * Clean and Shutdown the context environment. It must have been previously assigned with
	 * {@link this#assign(Environment)}.
	 *
	 * @throws java.lang.IllegalStateException if there is no environment initialized.
	 */
	public static void terminate() throws IllegalStateException {
		get().shutdown();
		enviromentReference.set(null);
	}

	/**
	 * Obtain the default timer from the current environment. The timer is created lazily so
	 * it is preferrable to fetch them out of the critical path.
	 * <p>
	 * The default timer is a {@link reactor.fn.timer.HashWheelTimer}. It is suitable for non blocking periodic work
	 * such as
	 * eventing, memory access, lock=free code, dispatching...
	 *
	 * @return the root timer, usually a {@link reactor.fn.timer.HashWheelTimer}
	 */
	public static Timer timer() {
		return get().getTimer();
	}

	/**
	 * Obtain the default dispatcher from the current environment. The dispatchers are created lazily so
	 * it is preferrable to fetch them out of the critical path.
	 * <p>
	 * The default dispatcher is considered the root or master dispatcher. It is encouraged to use it for non-blocking
	 * work,
	 * such as dispatching, memory access, lock-free code, eventing...
	 *
	 * @return the root dispatcher, usually a RingBufferDispatcher
	 */
	public static Dispatcher sharedDispatcher() {
		return get().getDefaultDispatcher();
	}

	/**
	 * Obtain a cached dispatcher out of {@link this#PROCESSORS} maximum pooled. The dispatchers are created lazily so
	 * it is preferrable to fetch them out of the critical path.
	 * <p>
	 * The Cached Dispatcher is suitable for IO work if combined with distinct reactor event buses {@link reactor.bus
	 * .EventBus} or
	 * streams {@link reactor.rx.Stream}.
	 *
	 * @return a dispatcher from the default pool, usually a RingBufferDispatcher.
	 */
	public static Dispatcher cachedDispatcher() {
		return get().getCachedDispatchers().get();
	}

	/**
	 * Obtain a registred dispatcher. The dispatchers are created lazily so
	 * it is preferrable to fetch them out of the critical path.
	 * <p>
	 * The Cached Dispatcher is suitable for IO work if combined with distinct reactor event buses {@link reactor.bus
	 * .EventBus} or
	 * streams {@link reactor.rx.Stream}.
	 *
	 * @param key the dispatcher name to find
	 * @return a dispatcher from the context environment registry.
	 */
	public static Dispatcher dispatcher(String key) {
		return get().getDispatcher(key);
	}

	/**
	 * Register a dispatcher into the context environment.
	 *
	 * @param key        the dispatcher name to use for future lookups
	 * @param dispatcher the dispatcher to register, if null, the key will be removed
	 * @return the passed dispatcher.
	 */
	public static Dispatcher dispatcher(String key, Dispatcher dispatcher) {
		if (dispatcher != null) {
			get().addDispatcher(key, dispatcher);
		} else {
			get().removeDispatcher(key);
		}
		return dispatcher;
	}

	/**
	 * Register a dispatcher into the context environment.
	 *
	 * @param key        the dispatcher configuration name to use to inherit properties from
	 * @return the new dispatcher.
	 */
	public static Dispatcher newDispatcherLike(String key) {
		return newDispatcherLike(key, null);
	}

	/**
	 * Register a dispatcher into the context environment.
	 *
	 * @param key        the dispatcher configuration name to use to inherit properties from
	 * @param newKey     the dispatcher name to use for future lookups
	 * @return the new dispatcher.
	 */
	public static Dispatcher newDispatcherLike(String key, String newKey) {
		Environment env = get();
		for(DispatcherConfiguration dispatcherConfiguration : env.configuration.getDispatcherConfigurations()){
			if(dispatcherConfiguration.getName().equals(key)){
				Dispatcher newDispatcher = initDispatcherFromConfiguration(dispatcherConfiguration);
				if(newKey != null && !newKey.isEmpty()){
					env.addDispatcher(newKey, newDispatcher);
				}
				return newDispatcher;
			}
		}
		throw new IllegalStateException("No dispatcher configuration found for "+key);
	}

	/**
	 * Register a dispatcher into the context environment. If it Unsafe friendly, will register a ringBuffer dispatcher,
	 * otherwise a simple MP-SC dispatcher.
	 * Will use a capacity of 2048 backlog elements.
	 *
	 * @return the new dispatcher.
	 */
	public static Dispatcher newDispatcher() {
		return newDispatcher(2048);
	}

	/**
	 * Register a dispatcher into the context environment. If it Unsafe friendly, will register a ringBuffer dispatcher,
	 * otherwise a simple MP-SC dispatcher.
	 *
	 * @param backlog the dispatcher capacity
	 * @return the new dispatcher.
	 */
	public static Dispatcher newDispatcher(int backlog) {
		return newDispatcher(null, backlog);
	}

	/**
	 * Register a dispatcher into the context environment. If it Unsafe friendly, will register a ringBuffer dispatcher,
	 * otherwise a simple MP-SC dispatcher.
	 *
	 * @param key     the dispatcher name to use for future lookups
	 * @param backlog the dispatcher capacity
	 * @return the passed dispatcher.
	 */
	public static Dispatcher newDispatcher(String key, int backlog) {
		return newDispatcher(key, backlog, 1, PlatformDependent.hasUnsafe() ? DispatcherType.RING_BUFFER : DispatcherType
				.MPSC);
	}

	/**
	 * Register a dispatcher into the context environment. If consumers greater than 1 and Unsafe is available,
	 * will register a WorkQueue Dispatcher, otherwise delegate to {@link Environment#newDispatcher(String, int)}
	 *
	 * @param backlog the dispatcher capacity
	 * @param consumers the dispatcher number of consumers
	 * @return the new dispatcher.
	 */
	public static Dispatcher newDispatcher(int backlog, int consumers) {
		return newDispatcher(null, backlog, consumers);
	}

	/**
	 Register a dispatcher into the context environment. If consumers greater than 1 and Unsafe is available,
	 * will register a WorkQueue Dispatcher, otherwise delegate to {@link Environment#newDispatcher(String, int)}
	 *
	 * @param key     the dispatcher name to use for future lookups
	 * @param backlog the dispatcher capacity
	 * @param consumers the dispatcher number of consumers
	 * @return the passed dispatcher.
	 */
	public static Dispatcher newDispatcher(String key, int backlog, int consumers) {
		if (consumers > 1 && PlatformDependent.hasUnsafe()) {
			return newDispatcher(key, backlog, consumers, DispatcherType.WORK_QUEUE);
		}
		return newDispatcher(key, backlog);
	}

	/**
	 * Register a dispatcher into the context environment.
	 *
	 * @param backlog        the dispatcher capacity
	 * @param consumers      the numbers of consumers
	 * @param dispatcherType the dispatcher type
	 * @return the new dispatcher.
	 */
	public static Dispatcher newDispatcher(int backlog, int consumers, DispatcherType dispatcherType) {
		return newDispatcher(null, backlog, consumers, dispatcherType);
	}

	/**
	 * Register a dispatcher into the context environment.
	 *
	 * @param key            the dispatcher name to use for future lookups
	 * @param backlog        the dispatcher capacity
	 * @param consumers      the numbers of consumers
	 * @param dispatcherType the dispatcher type
	 * @return the new dispatcher.
	 */
	public static Dispatcher newDispatcher(String key, int backlog, int consumers, DispatcherType dispatcherType) {
		Dispatcher dispatcher = initDispatcherFromConfiguration(new DispatcherConfiguration(key, dispatcherType, backlog,
				consumers));
		if (key != null && !key.isEmpty()) {
			Environment environment = get();
			environment.addDispatcher(key, dispatcher);
		}
		return dispatcher;
	}

	/**
	 * Obtain a dispatcher supplier into the context environment. Its main purpose is to cache dispatchers and produce
	 * them on request via {@link Supplier#get()}.
	 *
	 * @param key the dispatcher factory name to find
	 * @return a dispatcher factory registered with the passed key.
	 */
	public static DispatcherSupplier cachedDispatchers(String key) {
		return get().getCachedDispatchers(key);
	}

	/**
	 * Obtain the default dispatcher supplier from the context environment. Its main purpose is to cache dispatchers and
	 * produce
	 * them on request via {@link Supplier#get()}.
	 *
	 * @return a dispatcher factory registered with the default key.
	 */
	public static DispatcherSupplier cachedDispatchers() {
		return get().getCachedDispatchers();
	}

	/**
	 * Register a dispatcher supplier into the context environment. Its main purpose is to cache dispatchers and produce
	 * them on request via {@link Supplier#get()}.
	 *
	 * @param key                the dispatcher name to use for future lookups
	 * @param dispatcherSupplier the dispatcher factory to register, if null, the key will be removed
	 * @return a dispatcher from the default pool, usually a RingBufferDispatcher.
	 */
	public static DispatcherSupplier cachedDispatchers(String key, DispatcherSupplier dispatcherSupplier) {
		if (dispatcherSupplier != null) {
			get().addCachedDispatchers(key, dispatcherSupplier);
		} else {
			get().removeCachedDispatchers(key);
		}
		return dispatcherSupplier;
	}


	// INSTANCE

	private static final String DEFAULT_DISPATCHER_NAME = "__default-dispatcher";
	private static final String SYNC_DISPATCHER_NAME    = "sync";

	private final Properties env;

	private final AtomicReference<Timer>          timer               = new AtomicReference<Timer>();
	private final AtomicReference<EventBus>       rootBus             = new AtomicReference<EventBus>();
	private final Object                          monitor             = new Object();
	private final Filter                          dispatcherFilter    = new RoundRobinFilter();
	private final Map<String, DispatcherSupplier> dispatcherFactories = new HashMap<String, DispatcherSupplier>();

	private final ReactorConfiguration              configuration;
	private final MultiValueMap<String, Dispatcher> dispatchers;
	private final String                            defaultDispatcher;

	private volatile Consumer<? super Throwable> errorConsumer;

	/**
	 * Creates a new Environment that will use a {@link reactor.core.config.PropertiesConfigurationReader} to obtain its
	 * initial
	 * configuration. The configuration will be read from the classpath at the location {@code
	 * META-INF/reactor/default.properties}.
	 */
	public Environment() {
		this(Collections.<String, List<Dispatcher>>emptyMap(), new PropertiesConfigurationReader());
	}

	/**
	 * Creates a new Environment that will use the given {@code configurationReader} to obtain its initial configuration.
	 *
	 * @param configurationReader The configuration reader to use to obtain initial configuration
	 */
	public Environment(ConfigurationReader configurationReader) {
		this(Collections.<String, List<Dispatcher>>emptyMap(), configurationReader);
	}

	/**
	 * Creates a new Environment that will contain the given {@code dispatchers}, will use the given {@code
	 * configurationReader} to obtain additional configuration.
	 *
	 * @param dispatchers         The dispatchers to add include in the Environment
	 * @param configurationReader The configuration reader to use to obtain additional configuration
	 */
	public Environment(Map<String, List<Dispatcher>> dispatchers, ConfigurationReader configurationReader) {
		this.dispatchers = new LinkedMultiValueMap<String, Dispatcher>(dispatchers);

		configuration = configurationReader.read();
		defaultDispatcher = configuration.getDefaultDispatcherName() != null ? configuration.getDefaultDispatcherName() :
				DEFAULT_DISPATCHER_NAME;

		env = configuration.getAdditionalProperties();
	}

	public static DispatcherSupplier newCachedDispatchers(final int poolsize) {
		return newCachedDispatchers(poolsize, "parallel");
	}

	public static DispatcherSupplier newCachedDispatchers(final int poolsize, String name) {
		return createDispatcherFactory(name, poolsize, 1024, null, ProducerType.MULTI,
				new AgileWaitingStrategy());
	}

	public static DispatcherSupplier newFanOutCachedDispatchers(final int poolsize, String name) {
		return createDispatcherFactory(name, poolsize, 1024, null, ProducerType.SINGLE,
				new AgileWaitingStrategy());
	}

	private static ThreadPoolExecutorDispatcher createThreadPoolExecutorDispatcher(DispatcherConfiguration
			                                                                               dispatcherConfiguration) {
		int size = getSize(dispatcherConfiguration, 0);
		int backlog = getBacklog(dispatcherConfiguration, 128);

		return new ThreadPoolExecutorDispatcher(size,
				backlog,
				dispatcherConfiguration.getName());
	}

	private static WorkQueueDispatcher createWorkQueueDispatcher(DispatcherConfiguration dispatcherConfiguration) {
		int size = getSize(dispatcherConfiguration, 0);
		int backlog = getBacklog(dispatcherConfiguration, 16384);

		return new WorkQueueDispatcher("workQueueDispatcher",
				size,
				backlog,
				null);
	}

	private static RingBufferDispatcher createRingBufferDispatcher(DispatcherConfiguration dispatcherConfiguration) {
		int backlog = getBacklog(dispatcherConfiguration, 1024);
		return new RingBufferDispatcher(dispatcherConfiguration.getName(),
				backlog,
				null,
				ProducerType.MULTI,
				new AgileWaitingStrategy());
	}

	private static MpscDispatcher createMpscDispatcher(DispatcherConfiguration dispatcherConfiguration) {
		int backlog = getBacklog(dispatcherConfiguration, 1024);
		return new MpscDispatcher(dispatcherConfiguration.getName(), backlog);
	}

	private static int getBacklog(DispatcherConfiguration dispatcherConfiguration, int defaultBacklog) {
		Integer backlog = dispatcherConfiguration.getBacklog();
		if (null == backlog) {
			backlog = defaultBacklog;
		}
		return backlog;
	}

	private static int getSize(DispatcherConfiguration dispatcherConfiguration, int defaultSize) {
		Integer size = dispatcherConfiguration.getSize();
		if (null == size) {
			size = defaultSize;
		}
		if (size < 1) {
			size = PROCESSORS;
		}
		return size;
	}

	/**
	 * Gets the property with the given {@code key}. If the property does not exist {@code defaultValue} will be
	 * returned.
	 *
	 * @param key          The property key
	 * @param defaultValue The value to return if the property does not exist
	 * @return The value for the property
	 */
	public String getProperty(String key, String defaultValue) {
		return env.getProperty(key, defaultValue);
	}

	/**
	 * Gets the property with the given {@code key}, converting it to the required {@code type} using the {@link
	 * StandardConverters#CONVERTERS standard converters}. fF the property does not exist {@code defaultValue} will be
	 * returned.
	 *
	 * @param key          The property key
	 * @param type         The type to convert the property to
	 * @param defaultValue The value to return if the property does not exist
	 * @return The converted value for the property
	 */
	@SuppressWarnings("unchecked")
	public <T> T getProperty(String key, Class<T> type, T defaultValue) {
		Object val = env.getProperty(key);
		if (null == val) {
			return defaultValue;
		}
		if (!type.isAssignableFrom(val.getClass()) && StandardConverters.CONVERTERS.canConvert(String.class, type)) {
			return StandardConverters.CONVERTERS.convert(val, type);
		} else {
			return (T) val;
		}
	}

	/**
	 * Returns the default dispatcher for this environment. By default, when a {@link PropertiesConfigurationReader} is
	 * being used. This default dispatcher is specified by the value of the {@code reactor.dispatchers.default} property.
	 *
	 * @return The default dispatcher
	 */
	public Dispatcher getDefaultDispatcher() {
		return getDispatcher(defaultDispatcher);
	}

	/**
	 * Returns a default cached dispatcher for this environment. By default,
	 * when a {@link PropertiesConfigurationReader} is
	 * being used. This default dispatcher is specified by the value of the {@code reactor.dispatchers.dispatcherGroup}
	 * property.
	 *
	 * @return The next available dispatcher from default dispatcher group
	 * @since 2.0
	 */
	public Dispatcher getCachedDispatcher() {
		return getCachedDispatchers(DISPATCHER_GROUP).get();
	}

	/**
	 * Returns the default dispatcher group for this environment. By default,
	 * when a {@link PropertiesConfigurationReader} is
	 * being used. This default dispatcher is specified by the value of the {@code reactor.dispatchers.dispatcherGroup}
	 * property.
	 *
	 * @return The default dispatcher group
	 * @since 2.0
	 */
	public DispatcherSupplier getCachedDispatchers() {
		return getCachedDispatchers(DISPATCHER_GROUP);
	}

	/**
	 * Returns the dispatcher factory with the given {@code name}.
	 *
	 * @param name The name of the dispatcher factory
	 * @return The matching dispatcher factory, never {@code null}.
	 * @throws IllegalArgumentException if the dispatcher does not exist
	 */
	public DispatcherSupplier getCachedDispatchers(String name) {
		synchronized (monitor) {
			initDispatcherFactoryFromConfiguration(name);
			DispatcherSupplier factory = this.dispatcherFactories.get(name);
			if (factory == null) {
				throw new IllegalArgumentException("No Supplier<Dispatcher> found for name '" + name + "', " +
						"it must be present" +
						"in the configuration properties or being registered programmatically through this#addCachedDispatchers("
						+ name
						+ ", someDispatcherSupplier)");
			} else {
				return factory;
			}
		}
	}

	/**
	 * Returns the dispatcher with the given {@code name}.
	 *
	 * @param name The name of the dispatcher
	 * @return The matching dispatcher, never {@code null}.
	 * @throws IllegalArgumentException if the dispatcher does not exist
	 */
	public Dispatcher getDispatcher(String name) {
		if (name.equals(SYNC_DISPATCHER_NAME)) return SynchronousDispatcher.INSTANCE;

		synchronized (monitor) {
			initDispatcherFromConfiguration(name);
			List<Dispatcher> filteredDispatchers = Collections.emptyList();
			List<Dispatcher> dispatchers = this.dispatchers.get(name);
			if (dispatchers != null) {
				filteredDispatchers = this.dispatcherFilter.filter(dispatchers, name);
			}
			if (filteredDispatchers.isEmpty()) {
				throw new IllegalArgumentException("No Dispatcher found for name '" + name + "', it must be present " +
						"in the configuration properties or being registered programmatically through this#addDispatcher(" + name
						+ ", someDispatcher)");
			} else {
				return filteredDispatchers.get(0);
			}
		}
	}

	/**
	 * Route any exception to the environment error journal {@link this#errorConsumer}.
	 *
	 * @param throwable The error to route
	 * @return This Environment
	 */
	public void routeError(Throwable throwable) {
		Consumer<? super Throwable> errorJournal = errorConsumer;
		if (errorJournal != null) {
			errorJournal.accept(throwable);
		}
	}

	/**
	 * Assign a default error {@link Consumer} to listen for any call to {@link this#routeError(Throwable)}.
	 * The default journal will log through SLF4J Logger onto the category "reactor-environment".
	 *
	 * @return This Environment
	 */
	public Environment assignErrorJournal() {
		return assignErrorJournal(new Consumer<Throwable>() {
			Logger log = LoggerFactory.getLogger("reactor-environment");

			@Override
			public void accept(Throwable throwable) {
				log.error("", throwable);
			}
		});
	}

	/**
	 * Assign the error {@link Consumer} to listen for any call to {@link this#routeError(Throwable)}.
	 *
	 * @param errorJournal the consumer to listen for any exception
	 * @return This Environment
	 */
	public Environment assignErrorJournal(Consumer<? super Throwable> errorJournal) {
		this.errorConsumer = errorJournal;
		return this;
	}

	/**
	 * Adds the {@code dispatcher} to the environment, storing it using the given {@code name}.
	 *
	 * @param name       The name of the dispatcher
	 * @param dispatcher The dispatcher
	 * @return This Environment
	 */
	public Environment addDispatcher(String name, Dispatcher dispatcher) {
		synchronized (monitor) {
			this.dispatchers.add(name, dispatcher);
			if (name.equals(defaultDispatcher)) {
				this.dispatchers.add(DEFAULT_DISPATCHER_NAME, dispatcher);
			}
		}
		return this;
	}

	/**
	 * Adds the {@code dispatcherFactory} to the environment, storing it using the given {@code name}.
	 *
	 * @param name              The name of the dispatcher factory
	 * @param dispatcherFactory The dispatcher factory
	 * @return This Environment
	 */
	public Environment addCachedDispatchers(String name, DispatcherSupplier dispatcherFactory) {
		synchronized (monitor) {
			this.dispatcherFactories.put(name, dispatcherFactory);
		}
		return this;
	}

	/**
	 * Remove the {@code dispatcherFactory} to the environment keyed as the given {@code name}.
	 *
	 * @param name The name of the dispatcher factory to remove
	 * @return This Environment
	 */
	public Environment removeCachedDispatchers(String name) {
		synchronized (monitor) {
			this.dispatcherFactories.remove(name).shutdown();
		}
		return this;
	}

	/**
	 * Removes the Dispatcher, stored using the given {@code name} from the environment.
	 *
	 * @param name The name of the dispatcher
	 * @return This Environment
	 */
	public Environment removeDispatcher(String name) {
		synchronized (monitor) {
			for (Dispatcher dispatcher : dispatchers.remove(name)) {
				dispatcher.shutdown();
			}
		}
		return this;
	}

	/**
	 * Returns this environments root Reactor, creating it if necessary. The Reactor will use the environment default
	 * dispatcher.
	 *
	 * @return The root reactor
	 * @see Environment#getDefaultDispatcher()
	 */
	public EventBus getRootBus() {
		if (null == rootBus.get()) {
			synchronized (rootBus) {
				rootBus.compareAndSet(null, new EventBus(getDefaultDispatcher()));
			}
		}
		return rootBus.get();
	}

	/**
	 * Get the {@code Environment}-wide {@link reactor.fn.timer.SimpleHashWheelTimer}.
	 *
	 * @return the timer.
	 */
	public Timer getTimer() {
		if (null == timer.get()) {
			synchronized (timer) {
				Timer t = new HashWheelTimer();
				if (!timer.compareAndSet(null, t)) {
					t.cancel();
				}
			}
		}
		return timer.get();
	}

	/**
	 * Shuts down this Environment, causing all of its {@link Dispatcher Dispatchers} to be shut down.
	 *
	 * @see Dispatcher#shutdown
	 */
	public void shutdown() {
		List<Dispatcher> dispatchers = new ArrayList<Dispatcher>();
		synchronized (monitor) {
			for (Map.Entry<String, List<Dispatcher>> entry : this.dispatchers.entrySet()) {
				dispatchers.addAll(entry.getValue());
			}
		}
		for (Dispatcher dispatcher : dispatchers) {
			dispatcher.shutdown();
		}

		for (DispatcherSupplier dispatcherSupplier : dispatcherFactories.values()) {
			dispatcherSupplier.shutdown();
		}

		if (null != timer.get()) {
			timer.get().cancel();
		}
	}

	@Override
	public Iterator<Map.Entry<String, List<Dispatcher>>> iterator() {
		return this.dispatchers.entrySet().iterator();
	}

	@Override
	public void close() throws IOException {
		shutdown();
	}

	/**
	 * Create a RingBuffer pool that will clone up to {@param poolSize} generated dispatcher and return a different one
	 * on a round robin fashion each time {@link Supplier#get()} is called.
	 *
	 * @param name
	 * @param poolsize
	 * @param bufferSize
	 * @param errorHandler
	 * @param producerType
	 * @param waitStrategy
	 * @return
	 */
	public static DispatcherSupplier createDispatcherFactory(final String name,
	                                                         final int poolsize,
	                                                         final int bufferSize,
	                                                         final Consumer<Throwable> errorHandler,
	                                                         final ProducerType producerType,
	                                                         final WaitStrategy waitStrategy) {
		return new DispatcherSupplier() {
			int roundRobinIndex = -1;
			Dispatcher[] dispatchers = new Dispatcher[poolsize];
			boolean terminated = false;

			@Override
			public boolean alive() {
				return terminated;
			}

			@Override
			public void shutdown() {
				for (Dispatcher dispatcher : dispatchers) {
					if (dispatcher != null) {
						dispatcher.shutdown();
					}
				}
				terminated = true;
			}

			@Override
			public void forceShutdown() {
				for (Dispatcher dispatcher : dispatchers) {
					if (dispatcher != null) {
						dispatcher.forceShutdown();
					}
				}
				terminated = true;
			}

			@Override
			public Dispatcher get() {
				if (++roundRobinIndex == poolsize) {
					roundRobinIndex = 0;
				}
				if (dispatchers[roundRobinIndex] == null) {
					if (PlatformDependent.hasUnsafe()) {
						dispatchers[roundRobinIndex] = new RingBufferDispatcher(
								name,
								bufferSize,
								errorHandler,
								producerType,
								waitStrategy);
					} else {
						dispatchers[roundRobinIndex] = new MpscDispatcher(name, bufferSize);
					}
				}

				return dispatchers[roundRobinIndex];
			}
		};
	}


	private void initDispatcherFromConfiguration(String name) {
		if (dispatchers.get(name) != null) return;
		Dispatcher dispatcher;
		for (DispatcherConfiguration dispatcherConfiguration : configuration.getDispatcherConfigurations()) {
			if (!dispatcherConfiguration.getName().equalsIgnoreCase(name)) continue;

			dispatcher = initDispatcherFromConfiguration(dispatcherConfiguration);

			if (dispatcher != null) {
				addDispatcher(dispatcherConfiguration.getName(), dispatcher);
			}
		}
	}

	private static Dispatcher initDispatcherFromConfiguration(DispatcherConfiguration dispatcherConfiguration) {
		Dispatcher dispatcher = null;
		if (PlatformDependent.hasUnsafe() && DispatcherType.RING_BUFFER == dispatcherConfiguration.getType()) {
			dispatcher = createRingBufferDispatcher(dispatcherConfiguration);
		} else if (DispatcherType.RING_BUFFER == dispatcherConfiguration.getType() ||
				DispatcherType.MPSC == dispatcherConfiguration.getType()) {
			dispatcher = createMpscDispatcher(dispatcherConfiguration);
		} else if (DispatcherType.SYNCHRONOUS == dispatcherConfiguration.getType()) {
			dispatcher = SynchronousDispatcher.INSTANCE;
		} else if (DispatcherType.THREAD_POOL_EXECUTOR == dispatcherConfiguration.getType()) {
			dispatcher = createThreadPoolExecutorDispatcher(dispatcherConfiguration);
		} else if (DispatcherType.WORK_QUEUE == dispatcherConfiguration.getType()) {
			dispatcher = createWorkQueueDispatcher(dispatcherConfiguration);
		}

		return dispatcher;
	}

	private void initDispatcherFactoryFromConfiguration(String name) {
		if (dispatcherFactories.get(name) != null) return;
		for (DispatcherConfiguration dispatcherConfiguration : configuration.getDispatcherConfigurations()) {

			if (!dispatcherConfiguration.getName().equalsIgnoreCase(name)) continue;

			if (DispatcherType.DISPATCHER_GROUP == dispatcherConfiguration.getType()) {
				addCachedDispatchers(dispatcherConfiguration.getName(),
						createDispatcherFactory(
								dispatcherConfiguration.getName(),
								dispatcherConfiguration.getSize() == 0 ? PROCESSORS : dispatcherConfiguration.getSize(),
								dispatcherConfiguration.getBacklog(),
								null,
								ProducerType.MULTI,
								new AgileWaitingStrategy()
						));
			}
		}
	}

}
