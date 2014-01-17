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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import reactor.convert.StandardConverters;
import reactor.core.configuration.ConfigurationReader;
import reactor.core.configuration.DispatcherConfiguration;
import reactor.core.configuration.DispatcherType;
import reactor.core.configuration.PropertiesConfigurationReader;
import reactor.core.configuration.ReactorConfiguration;
import reactor.event.dispatch.BlockingQueueDispatcher;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.RingBufferDispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.dispatch.ThreadPoolExecutorDispatcher;
import reactor.filter.Filter;
import reactor.filter.RoundRobinFilter;
import reactor.util.LinkedMultiValueMap;
import reactor.util.MultiValueMap;
import reactor.event.Event;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Environment implements Iterable<Map.Entry<String, List<Dispatcher>>> {

	/**
	 * The name of the default event loop dispatcher
	 */
	public static final String EVENT_LOOP = "eventLoop";

	/**
	 * The name of the default ring buffer dispatcher
	 */
	public static final String RING_BUFFER = "ringBuffer";

	/**
	 * The name of the default thread pool dispatcher
	 */
	public static final String THREAD_POOL = "threadPoolExecutor";

	/**
	 * The number of processors available to the runtime
	 *
	 * @see Runtime#availableProcessors()
	 */
	public static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

	private static final String DEFAULT_DISPATCHER_NAME = "__default-dispatcher";
	private static final String SYNC_DISPATCHER_NAME    = "sync";
  private static final int    DEFAULT_INITIAL_POOL_SIZE = 1048;

	private final Properties env;

	private final HashWheelTimer           timer            = new HashWheelTimer();
	private final AtomicReference<Reactor> rootReactor      = new AtomicReference<Reactor>();
	private final Object                   monitor          = new Object();
	private final Filter                   dispatcherFilter = new RoundRobinFilter();

	private final MultiValueMap<String, Dispatcher> dispatchers;
	private final String                            defaultDispatcher;
  private final Event.GenericEventPool            eventPool;

	/**
	 * Creates a new Environment that will use a {@link PropertiesConfigurationReader} to obtain its initial
	 * configuration.
	 * The configuration will be read from the classpath at the location {@code META-INF/reactor/default.properties}.
	 */
	public Environment() {
		this(Collections.<String, List<Dispatcher>>emptyMap(), new PropertiesConfigurationReader());
	}

	/**
	 * Creates a new Environment that will use the given {@code configurationReader} to obtain its initial configuration.
	 *
	 * @param configurationReader
	 * 		The configuration reader to use to obtain initial configuration
	 */
	public Environment(ConfigurationReader configurationReader) {
		this(Collections.<String, List<Dispatcher>>emptyMap(), configurationReader);
	}

	/**
	 * Creates a new Environment that will contain the given {@code dispatchers}, will use the given {@code
	 * configurationReader} to obtain additional configuration.
	 *
	 * @param dispatchers
	 * 		The dispatchers to add include in the Environment
	 * @param configurationReader
	 * 		The configuration reader to use to obtain additional configuration
	 */
	public Environment(Map<String, List<Dispatcher>> dispatchers, ConfigurationReader configurationReader) {
		this.dispatchers = new LinkedMultiValueMap<String, Dispatcher>(dispatchers);

		ReactorConfiguration configuration = configurationReader.read();
		defaultDispatcher = configuration.getDefaultDispatcherName() != null ? configuration.getDefaultDispatcherName() :
		                    DEFAULT_DISPATCHER_NAME;
		env = configuration.getAdditionalProperties();

		for(DispatcherConfiguration dispatcherConfiguration : configuration.getDispatcherConfigurations()) {
			if(DispatcherType.EVENT_LOOP == dispatcherConfiguration.getType()) {
				int size = getSize(dispatcherConfiguration, 0);
				for(int i = 0; i < size; i++) {
					addDispatcher(dispatcherConfiguration.getName(), createBlockingQueueDispatcher(dispatcherConfiguration));
				}
			} else if(DispatcherType.RING_BUFFER == dispatcherConfiguration.getType()) {
				addDispatcher(dispatcherConfiguration.getName(), createRingBufferDispatcher(dispatcherConfiguration));
			} else if(DispatcherType.SYNCHRONOUS == dispatcherConfiguration.getType()) {
				addDispatcher(dispatcherConfiguration.getName(), new SynchronousDispatcher());
			} else if(DispatcherType.THREAD_POOL_EXECUTOR == dispatcherConfiguration.getType()) {
				addDispatcher(dispatcherConfiguration.getName(), createThreadPoolExecutorDispatcher(dispatcherConfiguration));
			}
		}

		addDispatcher(SYNC_DISPATCHER_NAME, new SynchronousDispatcher());
    this.eventPool = new Event.GenericEventPool(DEFAULT_INITIAL_POOL_SIZE);
  }

	private ThreadPoolExecutorDispatcher createThreadPoolExecutorDispatcher(DispatcherConfiguration dispatcherConfiguration) {
		int size = getSize(dispatcherConfiguration, 0);
		int backlog = getBacklog(dispatcherConfiguration, 128);

		return new ThreadPoolExecutorDispatcher(size, backlog, dispatcherConfiguration.getName());
	}

	private RingBufferDispatcher createRingBufferDispatcher(DispatcherConfiguration dispatcherConfiguration) {
		int backlog = getBacklog(dispatcherConfiguration, 1024);
		return new RingBufferDispatcher(dispatcherConfiguration.getName(),
		                                backlog,
		                                ProducerType.MULTI,
		                                new BlockingWaitStrategy());
	}

	private BlockingQueueDispatcher createBlockingQueueDispatcher(DispatcherConfiguration dispatcherConfiguration) {
		int backlog = getBacklog(dispatcherConfiguration, 128);

		return new BlockingQueueDispatcher(dispatcherConfiguration.getName(), backlog);
	}

	private int getBacklog(DispatcherConfiguration dispatcherConfiguration, int defaultBacklog) {
		Integer backlog = dispatcherConfiguration.getBacklog();
		if(null == backlog) {
			backlog = defaultBacklog;
		}
		return backlog;
	}

	private int getSize(DispatcherConfiguration dispatcherConfiguration, int defaultSize) {
		Integer size = dispatcherConfiguration.getSize();
		if(null == size) {
			size = defaultSize;
		}
		if(size < 1) {
			size = PROCESSORS;
		}
		return size;
	}

	/**
	 * Gets the property with the given {@code key}. If the property does not exist {@code defaultValue} will be
	 * returned.
	 *
	 * @param key
	 * 		The property key
	 * @param defaultValue
	 * 		The value to return if the property does not exist
	 *
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
	 * @param key
	 * 		The property key
	 * @param type
	 * 		The type to convert the property to
	 * @param defaultValue
	 * 		The value to return if the property does not exist
	 *
	 * @return The converted value for the property
	 */
	@SuppressWarnings("unchecked")
	public <T> T getProperty(String key, Class<T> type, T defaultValue) {
		Object val = env.getProperty(key);
		if(null == val) {
			return defaultValue;
		}
		if(!type.isAssignableFrom(val.getClass()) && StandardConverters.CONVERTERS.canConvert(String.class, type)) {
			return StandardConverters.CONVERTERS.convert(val, type);
		} else {
			return (T)val;
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
	 * Returns the dispatcher with the given {@code name}.
	 *
	 * @param name
	 * 		The name of the dispatcher
	 *
	 * @return The matching dispatcher, never {@code null}.
	 *
	 * @throws IllegalArgumentException
	 * 		if the dispatcher does not exist
	 */
	public Dispatcher getDispatcher(String name) {
		synchronized(monitor) {
			List<Dispatcher> dispatchers = this.dispatchers.get(name);
			List<Dispatcher> filteredDispatchers = this.dispatcherFilter.filter(dispatchers, name);
			if(filteredDispatchers.isEmpty()) {
				throw new IllegalArgumentException("No Dispatcher found for name '" + name + "'");
			} else {
				return filteredDispatchers.get(0);
			}
		}
	}

	/**
	 * Adds the {@code dispatcher} to the environment, storing it using the given {@code name}.
	 *
	 * @param name
	 * 		The name of the dispatcher
	 * @param dispatcher
	 * 		The dispatcher
	 *
	 * @return This Environment
	 */
	public Environment addDispatcher(String name, Dispatcher dispatcher) {
		synchronized(monitor) {
			this.dispatchers.add(name, dispatcher);
			if(name.equals(defaultDispatcher)) {
				this.dispatchers.add(DEFAULT_DISPATCHER_NAME, dispatcher);
			}
		}
		return this;
	}

	/**
	 * Removes the Dispatcher, stored using the given {@code name} from the environment.
	 *
	 * @param name
	 * 		The name of the dispatcher
	 *
	 * @return This Environment
	 */
	public Environment removeDispatcher(String name) {
		synchronized(monitor) {
			dispatchers.remove(name);
		}
		return this;
	}

	/**
	 * Returns this environments root Reactor, creating it if necessary. The Reactor will use the environment default
	 * dispatcher.
	 *
	 * @return The root reactor
	 *
	 * @see Environment#getDefaultDispatcher()
	 */
	public Reactor getRootReactor() {
		rootReactor.compareAndSet(null, new Reactor(getDefaultDispatcher(), eventPool));
		return rootReactor.get();
	}

	public HashWheelTimer getRootTimer() {
		return timer;
	}

	/**
	 * Shuts down this Environment, causing all of its {@link Dispatcher Dispatchers} to be shut down.
	 *
	 * @see Dispatcher#shutdown
	 */
	public void shutdown() {
		List<Dispatcher> dispatchers = new ArrayList<Dispatcher>();
		synchronized(monitor) {
			for(Map.Entry<String, List<Dispatcher>> entry : this.dispatchers.entrySet()) {
				dispatchers.addAll(entry.getValue());
			}
		}
		for(Dispatcher dispatcher : dispatchers) {
			dispatcher.shutdown();
		}
		timer.cancel();
	}

	@Override
	public Iterator<Map.Entry<String, List<Dispatcher>>> iterator() {
		return this.dispatchers.entrySet().iterator();
	}

  public Event.GenericEventPool getEventPool() {
    return this.eventPool;
  }
}
