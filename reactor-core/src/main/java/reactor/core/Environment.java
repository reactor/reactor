package reactor.core;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import reactor.convert.StandardConverters;
import reactor.fn.*;
import reactor.fn.dispatch.*;

import java.io.IOException;
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
public class Environment implements Observable {

	private static final ClassLoader CL = Environment.class.getClassLoader();

	private static final String REACTOR_PREFIX      = "reactor.";
	private static final String PROFILES_ACTIVE     = "reactor.profiles.active";
	private static final String PROFILES_DEFAULT    = "reactor.profiles.default";
	private static final String DISPATCHERS         = "reactor.dispatchers.%s";
	private static final String DISPATCHERS_NAME    = "reactor.dispatchers.%s.name";
	private static final String DISPATCHERS_SIZE    = "reactor.dispatchers.%s.size";
	private static final String DISPATCHERS_BACKLOG = "reactor.dispatchers.%s.backlog";

	public static final String THREAD_POOL_EXECUTOR_DISPATCHER = "threadPoolExecutor";
	public static final String EVENT_LOOP_DISPATCHER           = "eventLoop";
	public static final String RING_BUFFER_DISPATCHER          = "ringBuffer";
	public static final String REACTOR_START                   = "reactor.start";

	public static final int PROCESSORS = Runtime.getRuntime().availableProcessors();

	private final Properties               env           = new Properties();
	private final Registry<Reactor>        reactors      = new CachingRegistry<Reactor>(null, null);
	private final AtomicReference<Reactor> globalReactor = new AtomicReference<Reactor>();

	public Environment() {
		for (String prop : System.getProperties().stringPropertyNames()) {
			if (prop.startsWith(REACTOR_PREFIX)) {
				env.put(prop, System.getProperty(prop));
			}
		}

		String defaultProfileName = System.getProperty(PROFILES_DEFAULT, getDefaultProfile());
		try {
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
				env.put(String.format(DISPATCHERS, threadPoolExecutorName),
								new SingletonDispatcherSupplier(new ThreadPoolExecutorDispatcher(size, backlog).start()));
			}

			String eventLoopName = env.getProperty(String.format(DISPATCHERS_NAME, EVENT_LOOP_DISPATCHER));
			if (null != eventLoopName) {
				int size = getProperty(String.format(DISPATCHERS_SIZE, eventLoopName), Integer.class, PROCESSORS);
				if (size < 1) {
					size = PROCESSORS;
				}
				int backlog = getProperty(String.format(DISPATCHERS_BACKLOG, threadPoolExecutorName), Integer.class, 128);
				Dispatcher[] dispatchers = new Dispatcher[size];
				for (int i = 0; i < size; i++) {
					dispatchers[i] = new BlockingQueueDispatcher(eventLoopName, backlog).start();
				}
				env.put(String.format(DISPATCHERS, eventLoopName),
								new RoundRobinDispatcherSupplier(dispatchers));
			}

			String ringBufferName = env.getProperty(String.format(DISPATCHERS_NAME, RING_BUFFER_DISPATCHER));
			if (null != ringBufferName) {
				int size = getProperty(String.format(DISPATCHERS_SIZE, ringBufferName), Integer.class, PROCESSORS);
				if (size < 1) {
					size = PROCESSORS;
				}
				int backlog = getProperty(String.format(DISPATCHERS_BACKLOG, ringBufferName), Integer.class, 1024);
				env.put(String.format(DISPATCHERS, threadPoolExecutorName),
								new SingletonDispatcherSupplier(new RingBufferDispatcher(ringBufferName,
																																				 size,
																																				 backlog,
																																				 ProducerType.MULTI,
																																				 new BlockingWaitStrategy()).start()));
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
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

	public DispatcherSupplier getDispatcherSupplier(String name) {
		String dispatcherName = String.format(DISPATCHERS, name);
		DispatcherSupplier supplier = (DispatcherSupplier) env.get(dispatcherName);
		if (null == supplier) {
			throw new IllegalArgumentException("No DispatcherSupplier found for name '" + name + "'");
		}
		return supplier;
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

	public Reactor remove(String id) {
		return remove(UUID.fromString(id));
	}

	public Reactor remove(UUID id) {
		Iterator<Registration<? extends Reactor>> rs = reactors.select(id).iterator();
		if (!rs.hasNext()) {
			return null;
		}

		Registration<? extends Reactor> reg = rs.next();
		Reactor r = reg.getObject();
		reg.cancel();

		return r;
	}

	@Override
	public boolean respondsToKey(Object key) {
		return global().respondsToKey(key);
	}

	@Override
	public <T, E extends Event<T>> Registration<Consumer<E>> on(Selector sel, Consumer<E> consumer) {
		return global().on(sel, consumer);
	}

	@Override
	public <T, E extends Event<T>> Registration<Consumer<E>> on(Consumer<E> consumer) {
		return global().on(consumer);
	}

	@Override
	public <T, E extends Event<T>, V> Registration<Consumer<E>> receive(Selector sel, Function<E, V> fn) {
		return global().receive(sel, fn);
	}

	@Override
	public <T, E extends Event<T>> Observable notify(Object key, E ev, Consumer<E> onComplete) {
		return global().notify(key, ev, onComplete);
	}

	@Override
	public <T, E extends Event<T>> Observable notify(Object key, E ev) {
		return global().notify(key, ev);
	}

	@Override
	public <T, S extends Supplier<Event<T>>> Observable notify(Object key, S supplier) {
		return global().notify(key, supplier);
	}

	@Override
	public <T, E extends Event<T>> Observable notify(E ev) {
		return global().notify(ev);
	}

	@Override
	public <T, S extends Supplier<Event<T>>> Observable notify(S supplier) {
		return global().notify(supplier);
	}

	@Override
	public <T, E extends Event<T>> Observable send(Object key, E ev) {
		return global().send(key, ev);
	}

	@Override
	public <T, S extends Supplier<Event<T>>> Observable send(Object key, S supplier) {
		return global().send(key, supplier);
	}

	@Override
	public <T, E extends Event<T>> Observable send(Object key, E ev, Observable replyTo) {
		return global().send(key, ev, replyTo);
	}

	@Override
	public <T, S extends Supplier<Event<T>>> Observable send(Object key, S supplier, Observable replyTo) {
		return global().send(key, supplier, replyTo);
	}

	@Override
	public Observable notify(Object key) {
		return global().notify(key);
	}

	protected String getDefaultProfile() {
		return "default";
	}

	protected Properties loadProfile(String name) throws IOException {
		Properties props = new Properties();
		props.load(CL.getResourceAsStream(String.format("META-INF/reactor/%s.properties", name)));
		return props;
	}

	private Reactor global() {
		globalReactor.compareAndSet(null, new Reactor(this, new BlockingQueueDispatcher("env", 128).start()));
		return globalReactor.get();
	}

}
