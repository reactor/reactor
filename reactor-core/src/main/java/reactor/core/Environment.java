package reactor.core;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.LoggerFactory;
import reactor.convert.StandardConverters;
import reactor.fn.Registration;
import reactor.fn.Registry;
import reactor.fn.dispatch.BlockingQueueDispatcher;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.dispatch.RingBufferDispatcher;
import reactor.fn.dispatch.ThreadPoolExecutorDispatcher;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static reactor.Fn.$;

/**
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public class Environment {

	private static final ClassLoader CL = Environment.class.getClassLoader();

	private static final String REACTOR_PREFIX      = "reactor.";
	private static final String PROFILES_ACTIVE     = "reactor.profiles.active";
	private static final String PROFILES_DEFAULT    = "reactor.profiles.default";
	private static final String DISPATCHERS_NAME    = "reactor.dispatchers.%s.name";
	private static final String DISPATCHERS_SIZE    = "reactor.dispatchers.%s.size";
	private static final String DISPATCHERS_BACKLOG = "reactor.dispatchers.%s.backlog";

	public static final String THREAD_POOL_EXECUTOR_DISPATCHER = "threadPoolExecutor";
	public static final String EVENT_LOOP_DISPATCHER           = "eventLoop";
	public static final String RING_BUFFER_DISPATCHER          = "ringBuffer";

	public static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

	private final Properties               env           = new Properties();
	private final AtomicReference<Reactor> sharedReactor = new AtomicReference<Reactor>();
	private final Registry<Reactor>        reactors      = new CachingRegistry<Reactor>(null, null);
	private final Registry<Dispatcher> dispatcherSuppliers;

	public Environment() {
		this(new CachingRegistry<Dispatcher>(Registry.LoadBalancingStrategy.ROUND_ROBIN, null));
	}

	public Environment(Registry<Dispatcher> dispatcherSuppliers) {
		String defaultProfileName = System.getProperty(PROFILES_DEFAULT, getDefaultProfile());
		Map<Object, Object> props = loadProfile(defaultProfileName);
		if (null != props) {
			env.putAll(props);
		}

		if (null != System.getProperty(PROFILES_ACTIVE)) {
			String[] profiles = System.getProperty(PROFILES_ACTIVE).split(",");
			for (String profile : profiles) {
				props = loadProfile(profile);
				if (null != props) {
					env.putAll(props);
				}
			}
		}

		String threadPoolExecutorName = env.getProperty(String.format(DISPATCHERS_NAME, THREAD_POOL_EXECUTOR_DISPATCHER));
		if (null != threadPoolExecutorName) {
			int size = getProperty(String.format(DISPATCHERS_SIZE, threadPoolExecutorName), Integer.class, PROCESSORS);
			if (size < 1) {
				size = PROCESSORS;
			}
			int backlog = getProperty(String.format(DISPATCHERS_BACKLOG, threadPoolExecutorName), Integer.class, 128);
			dispatcherSuppliers.register($(threadPoolExecutorName),
																	 new ThreadPoolExecutorDispatcher(size, backlog).start());
		}

		String eventLoopName = env.getProperty(String.format(DISPATCHERS_NAME, EVENT_LOOP_DISPATCHER));
		if (null != eventLoopName) {
			int size = getProperty(String.format(DISPATCHERS_SIZE, eventLoopName), Integer.class, PROCESSORS);
			if (size < 1) {
				size = PROCESSORS;
			}
			int backlog = getProperty(String.format(DISPATCHERS_BACKLOG, threadPoolExecutorName), Integer.class, 128);
			for (int i = 0; i < size; i++) {
				dispatcherSuppliers.register($(eventLoopName),
																		 new BlockingQueueDispatcher(eventLoopName, backlog).start());
			}
		}

		String ringBufferName = env.getProperty(String.format(DISPATCHERS_NAME, RING_BUFFER_DISPATCHER));
		if (null != ringBufferName) {
			int size = getProperty(String.format(DISPATCHERS_SIZE, ringBufferName), Integer.class, PROCESSORS);
			if (size < 1) {
				size = PROCESSORS;
			}
			int backlog = getProperty(String.format(DISPATCHERS_BACKLOG, ringBufferName), Integer.class, 1024);
			dispatcherSuppliers.register($(ringBufferName),
																	 new RingBufferDispatcher(ringBufferName,
																														size,
																														backlog,
																														ProducerType.MULTI,
																														new BlockingWaitStrategy()).start());
		}

		this.dispatcherSuppliers = dispatcherSuppliers;

		for (String prop : System.getProperties().stringPropertyNames()) {
			if (prop.startsWith(REACTOR_PREFIX)) {
				env.put(prop, System.getProperty(prop));
			}
		}
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

	public Dispatcher getDispatcher(String name) {
		Iterator<Registration<? extends Dispatcher>> regs = dispatcherSuppliers.select(name).iterator();
		if (!regs.hasNext()) {
			throw new IllegalArgumentException("No DispatcherSupplier found for name '" + name + "'");
		}
		return regs.next().getObject();
	}

	public Registration<? extends Reactor> register(Reactor reactor) {
		return reactors.register($(reactor.getId()), reactor);
	}

	public Reactor find(String id) {
		return find(UUID.fromString(id));
	}

	public Reactor find(UUID id) {
		Iterator<Registration<? extends Reactor>> rs = reactors.select(id).iterator();
		if (!rs.hasNext()) {
			return null;
		}

		Reactor r = null;
		while (rs.hasNext()) {
			r = rs.next().getObject();
		}
		return r;
	}

	public boolean unregister(String id) {
		return unregister(UUID.fromString(id));
	}

	public boolean unregister(UUID id) {
		return reactors.unregister(id);
	}

	public Reactor getSharedReactor() {
		sharedReactor.compareAndSet(null, new Reactor(this, new BlockingQueueDispatcher("env", 128).start()));
		return sharedReactor.get();
	}

	protected String getDefaultProfile() {
		return "default";
	}

	protected Properties loadProfile(String name) {
		Properties props = new Properties();
		URL propsUrl = CL.getResource(String.format("META-INF/reactor/%s.properties", name));
		if (null != propsUrl) {
			try {
				props.load(propsUrl.openStream());
			} catch (IOException e) {
				LoggerFactory.getLogger(Environment.class).error(e.getMessage(), e);
			}
		}
		return props;
	}

}
