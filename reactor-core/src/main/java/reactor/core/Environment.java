/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core;

import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import reactor.convert.StandardConverters;
import reactor.core.configuration.*;
import reactor.event.dispatch.*;
import reactor.event.dispatch.wait.AgileWaitingStrategy;
import reactor.filter.Filter;
import reactor.filter.RoundRobinFilter;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.timer.SimpleHashWheelTimer;
import reactor.timer.Timer;
import reactor.util.LinkedMultiValueMap;
import reactor.util.MultiValueMap;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Environment implements Iterable<Map.Entry<String, List<Dispatcher>>> {

	/**
	 * The name of the default ring buffer group dispatcher
	 */
	public static final String RING_BUFFER_GROUP = "ringBufferGroup";

	/**
	 * The name of the default ring buffer dispatcher
	 */
	public static final String RING_BUFFER = "ringBuffer";

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
	public static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

	private static final String DEFAULT_DISPATCHER_NAME = "__default-dispatcher";
	private static final String SYNC_DISPATCHER_NAME    = "sync";

	private final Properties env;

	private final AtomicReference<Timer>            timer               = new AtomicReference<Timer>();
	private final AtomicReference<Reactor>          rootReactor         = new AtomicReference<Reactor>();
	private final Object                            monitor             = new Object();
	private final Filter                            dispatcherFilter    = new RoundRobinFilter();
	private final Map<String, Supplier<Dispatcher>> dispatcherFactories = new HashMap<String, Supplier<Dispatcher>>();

	private final ReactorConfiguration              configuration;
	private final MultiValueMap<String, Dispatcher> dispatchers;
	private final String                            defaultDispatcher;

	/**
	 * Creates a new Environment that will use a {@link PropertiesConfigurationReader} to obtain its initial
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

		addDispatcher(SYNC_DISPATCHER_NAME, SynchronousDispatcher.INSTANCE);
	}

	public static Supplier<Dispatcher> newDispatcherFactory(final int poolsize) {
		return newDispatcherFactory(poolsize, "parallel");
	}

	public static Supplier<Dispatcher> newDispatcherFactory(final int poolsize, String name) {
		return createDispatcherFactory(name, poolsize, 1024, null, ProducerType.MULTI,
				new AgileWaitingStrategy());
	}

	public static Supplier<Dispatcher> newSingleProducerMultiConsumerDispatcherFactory(final int poolsize, String name) {
		return createDispatcherFactory(name, poolsize, 1024, null, ProducerType.SINGLE,
				new AgileWaitingStrategy());
	}

	private ThreadPoolExecutorDispatcher createThreadPoolExecutorDispatcher(DispatcherConfiguration
			                                                                        dispatcherConfiguration) {
		int size = getSize(dispatcherConfiguration, 0);
		int backlog = getBacklog(dispatcherConfiguration, 128);

		return new ThreadPoolExecutorDispatcher(size,
				backlog,
				dispatcherConfiguration.getName());
	}

	private WorkQueueDispatcher createWorkQueueDispatcher(DispatcherConfiguration dispatcherConfiguration) {
		int size = getSize(dispatcherConfiguration, 0);
		int backlog = getBacklog(dispatcherConfiguration, 16384);

		return new WorkQueueDispatcher("workQueueDispatcher",
				size,
				backlog,
				null);
	}

	private RingBufferDispatcher createRingBufferDispatcher(DispatcherConfiguration dispatcherConfiguration) {
		int backlog = getBacklog(dispatcherConfiguration, 1024);
		return new RingBufferDispatcher(dispatcherConfiguration.getName(),
				backlog,
				null,
				ProducerType.MULTI,
				new AgileWaitingStrategy());
	}

	private int getBacklog(DispatcherConfiguration dispatcherConfiguration, int defaultBacklog) {
		Integer backlog = dispatcherConfiguration.getBacklog();
		if (null == backlog) {
			backlog = defaultBacklog;
		}
		return backlog;
	}

	private int getSize(DispatcherConfiguration dispatcherConfiguration, int defaultSize) {
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
	 * Returns the default dispatcher group for this environment. By default,
	 * when a {@link PropertiesConfigurationReader} is
	 * being used. This default dispatcher is specified by the value of the {@code reactor.dispatchers.ringBufferGroup}
	 * property.
	 *
	 * @return The default dispatcher group
	 * @since 2.0
	 */
	public Supplier<Dispatcher> getDefaultDispatcherFactory() {
		return getDispatcherFactory(RING_BUFFER_GROUP);
	}

	/**
	 * Returns the dispatcher factory with the given {@code name}.
	 *
	 * @param name The name of the dispatcher factory
	 * @return The matching dispatcher factory, never {@code null}.
	 * @throws IllegalArgumentException if the dispatcher does not exist
	 */
	public Supplier<Dispatcher> getDispatcherFactory(String name) {
		synchronized (monitor) {
			initDispatcherFactoryFromConfiguration(name);
			Supplier<Dispatcher> factory = this.dispatcherFactories.get(name);
			if (factory == null) {
				throw new IllegalArgumentException("No Supplier<Dispatcher> found for name '" + name + "', " +
						"it must be present" +
						"in the configuration properties or being registered programmatically through this#addDispatcherFactory("
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
		synchronized (monitor) {
			initDispatcherFromConfiguration(name);
			List<Dispatcher> filteredDispatchers = Collections.emptyList();
			List<Dispatcher> dispatchers = this.dispatchers.get(name);
			if (dispatchers != null) {
				filteredDispatchers = this.dispatcherFilter.filter(dispatchers, name);
			}
			if (filteredDispatchers.isEmpty()) {
				throw new IllegalArgumentException("No Dispatcher found for name '" + name + "', it must be present" +
						"in the configuration properties or being registered programmatically through this#addDispatcher(" + name
						+ ", someDispatcher)");
			} else {
				return filteredDispatchers.get(0);
			}
		}
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
	 * Adds the {@code dispatcher} to the environment, storing it using the given {@code name}.
	 *
	 * @param name              The name of the dispatcher
	 * @param dispatcherFactory The dispatcher factory
	 * @return This Environment
	 */
	public Environment addDispatcherFactory(String name, Supplier<Dispatcher> dispatcherFactory) {
		synchronized (monitor) {
			this.dispatcherFactories.put(name, dispatcherFactory);
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
			dispatchers.remove(name);
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
	public Reactor getRootReactor() {
		if (null == rootReactor.get()) {
			synchronized (rootReactor) {
				rootReactor.compareAndSet(null, new Reactor(getDefaultDispatcher()));
			}
		}
		return rootReactor.get();
	}

	/**
	 * Get the {@code Environment}-wide {@link reactor.timer.SimpleHashWheelTimer}.
	 *
	 * @return the timer.
	 */
	public Timer getRootTimer() {
		if (null == timer.get()) {
			synchronized (timer) {
				SimpleHashWheelTimer t = new SimpleHashWheelTimer();
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
		if (null != timer.get()) {
			timer.get().cancel();
		}
	}

	@Override
	public Iterator<Map.Entry<String, List<Dispatcher>>> iterator() {
		return this.dispatchers.entrySet().iterator();
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
	public static Supplier<Dispatcher> createDispatcherFactory(final String name,
	                                                           final int poolsize,
	                                                           final int bufferSize,
	                                                           final Consumer<Throwable> errorHandler,
	                                                           final ProducerType producerType,
	                                                           final WaitStrategy waitStrategy) {
		return new Supplier<Dispatcher>() {
			int roundRobinIndex = -1;
			Dispatcher[] dispatchers = new Dispatcher[poolsize];

			@Override
			public Dispatcher get() {
				if (++roundRobinIndex == poolsize) {
					roundRobinIndex = 0;
				}
				if (dispatchers[roundRobinIndex] == null) {
					dispatchers[roundRobinIndex] = new RingBufferDispatcher(
							name,
							bufferSize,
							errorHandler,
							producerType,
							waitStrategy);
				}

				return dispatchers[roundRobinIndex];
			}
		};
	}


	private void initDispatcherFromConfiguration(String name) {
		if (dispatchers.get(name) != null) return;

		for (DispatcherConfiguration dispatcherConfiguration : configuration.getDispatcherConfigurations()) {

			if (!dispatcherConfiguration.getName().equalsIgnoreCase(name)) continue;

			if (DispatcherType.RING_BUFFER == dispatcherConfiguration.getType()) {
				addDispatcher(dispatcherConfiguration.getName(), createRingBufferDispatcher(dispatcherConfiguration));
			} else if (DispatcherType.SYNCHRONOUS == dispatcherConfiguration.getType()) {
				addDispatcher(dispatcherConfiguration.getName(), SynchronousDispatcher.INSTANCE);
			} else if (DispatcherType.THREAD_POOL_EXECUTOR == dispatcherConfiguration.getType()) {
				addDispatcher(dispatcherConfiguration.getName(), createThreadPoolExecutorDispatcher(dispatcherConfiguration));
			} else if (DispatcherType.WORK_QUEUE == dispatcherConfiguration.getType()) {
				addDispatcher(dispatcherConfiguration.getName(), createWorkQueueDispatcher(dispatcherConfiguration));
			}
		}
	}

	private void initDispatcherFactoryFromConfiguration(String name) {
		if (dispatcherFactories.get(name) != null) return;
		for (DispatcherConfiguration dispatcherConfiguration : configuration.getDispatcherConfigurations()) {

			if (!dispatcherConfiguration.getName().equalsIgnoreCase(name)) continue;

			if (DispatcherType.RING_BUFFER_GROUP == dispatcherConfiguration.getType()) {
				addDispatcherFactory(dispatcherConfiguration.getName(),
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
