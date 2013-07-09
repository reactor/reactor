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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import reactor.convert.StandardConverters;
import reactor.core.configuration.ConfigurationReader;
import reactor.core.configuration.DispatcherConfiguration;
import reactor.core.configuration.DispatcherType;
import reactor.core.configuration.PropertiesConfigurationReader;
import reactor.core.configuration.ReactorConfiguration;
import reactor.filter.Filter;
import reactor.filter.RoundRobinFilter;
import reactor.event.dispatch.BlockingQueueDispatcher;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.RingBufferDispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.dispatch.ThreadPoolExecutorDispatcher;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Environment {

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

	public static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

	private static final String DEFAULT_DISPATCHER_NAME = "__default-dispatcher";
	private static final String SYNC_DISPATCHER_NAME    = "sync";

	private final Properties env;

	private final AtomicReference<Reactor> rootReactor      = new AtomicReference<Reactor>();
	private final Object                   monitor          = new Object();
	private final Filter                   dispatcherFilter = new RoundRobinFilter();

	private final Map<String, List<Dispatcher>> dispatchers;
	private final String                        defaultDispatcher;

	public Environment() {
		this(Collections.<String, List<Dispatcher>>emptyMap(), new PropertiesConfigurationReader());
	}

	public Environment(ConfigurationReader configurationReader) {
		this(Collections.<String, List<Dispatcher>>emptyMap(), configurationReader);
	}

	public Environment(Map<String, List<Dispatcher>> dispatchers, ConfigurationReader configurationReader) {

		this.dispatchers = new HashMap<String, List<Dispatcher>>(dispatchers);

		ReactorConfiguration configuration = configurationReader.read();
		defaultDispatcher = configuration.getDefaultDispatcherName();
		env = configuration.getAdditionalProperties();

		for (DispatcherConfiguration dispatcherConfiguration : configuration.getDispatcherConfigurations()) {
			if (DispatcherType.EVENT_LOOP == dispatcherConfiguration.getType()) {
				addDispatcher(dispatcherConfiguration.getName(), createBlockingQueueDispatcher(dispatcherConfiguration));
			} else if (DispatcherType.RING_BUFFER == dispatcherConfiguration.getType()) {
				addDispatcher(dispatcherConfiguration.getName(), createRingBufferDispatcher(dispatcherConfiguration));
			} else if (DispatcherType.SYNCHRONOUS == dispatcherConfiguration.getType()) {
				addDispatcher(dispatcherConfiguration.getName(), new SynchronousDispatcher());
			} else if (DispatcherType.THREAD_POOL_EXECUTOR == dispatcherConfiguration.getType()) {
				addDispatcher(dispatcherConfiguration.getName(), createThreadPoolExecutorDispatcher(dispatcherConfiguration));
			}
		}

		addDispatcher(SYNC_DISPATCHER_NAME, new SynchronousDispatcher());
	}

	private ThreadPoolExecutorDispatcher createThreadPoolExecutorDispatcher(DispatcherConfiguration dispatcherConfiguration) {
		int size = getSize(dispatcherConfiguration, 0);
		int backlog = getBacklog(dispatcherConfiguration, 128);

		return new ThreadPoolExecutorDispatcher(size, backlog);
	}

	private RingBufferDispatcher createRingBufferDispatcher(DispatcherConfiguration dispatcherConfiguration) {
		int backlog = getBacklog(dispatcherConfiguration, 1024);
		return new RingBufferDispatcher(dispatcherConfiguration.getName(), backlog, ProducerType.MULTI, new BlockingWaitStrategy());
	}

	private BlockingQueueDispatcher createBlockingQueueDispatcher(DispatcherConfiguration dispatcherConfiguration) {
		int backlog = getBacklog(dispatcherConfiguration, 128);

		return new BlockingQueueDispatcher(dispatcherConfiguration.getName(), backlog);
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

	public String getProperty(String key, String defaultValue) {
		return env.getProperty(key, defaultValue);
	}

	@SuppressWarnings("unchecked")
	public <T> T getProperty(String key, Class<T> type, T defaultValue) {
		if (env.containsKey(key)) {
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
		return defaultValue;
	}

	public Dispatcher getDefaultDispatcher() {
		return getDispatcher(DEFAULT_DISPATCHER_NAME);
	}

	public Dispatcher getDispatcher(String name) {
		synchronized (monitor) {
			List<Dispatcher> dispatchers = this.dispatchers.get(name);
			List<Dispatcher> filteredDispatchers = this.dispatcherFilter.filter(dispatchers, name);
			if (filteredDispatchers.isEmpty()) {
				throw new IllegalArgumentException("No Dispatcher found for name '" + name + "'");
			} else {
				return filteredDispatchers.get(0);
			}
		}
	}

	public Environment addDispatcher(String name, Dispatcher dispatcher) {
		synchronized (monitor) {
			doAddDispatcher(name, dispatcher);
			if (name.equals(defaultDispatcher)) {
				doAddDispatcher(DEFAULT_DISPATCHER_NAME, dispatcher);
			}
		}
		return this;
	}

	private void doAddDispatcher(String name, Dispatcher dispatcher) {
		List<Dispatcher> dispatchers = this.dispatchers.get(name);
		if (dispatchers == null) {
			dispatchers = new ArrayList<Dispatcher>();
			this.dispatchers.put(name, dispatchers);
		}
		dispatchers.add(dispatcher);
	}

	public Environment removeDispatcher(String name) {
		synchronized (monitor) {
			dispatchers.remove(name);
		}
		return this;
	}

	public Reactor getRootReactor() {
		rootReactor.compareAndSet(null, new Reactor(getDefaultDispatcher()));
		return rootReactor.get();
	}

	/**
	 * Shuts down this Environment, causing all of its {@link Dispatcher Dispatchers} to be shut down.
	 *
	 * @see Dispatcher#shutdown
	 */
	public void shutdown() {
		List<Dispatcher> dispatchers = new ArrayList<Dispatcher>();
		synchronized(monitor) {
			for (Map.Entry<String, List<Dispatcher>> entry : this.dispatchers.entrySet()) {
				dispatchers.addAll(entry.getValue());
			}
		}
		for (Dispatcher dispatcher: dispatchers) {
			dispatcher.shutdown();
		}
	}
}
