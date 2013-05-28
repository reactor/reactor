package reactor.core;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 */
public class Environment {

	private final int eventsLoops;
	private final AtomicLong                               nextDispatcherCounter = new AtomicLong(Long.MIN_VALUE);
	private final NonBlockingHashMap<String, ReactorEntry> reactors              = new NonBlockingHashMap<String,
			ReactorEntry>();

	public Environment() {
		this(Runtime.getRuntime().availableProcessors());
	}

	public Environment(int eventsLoops) {
		this.eventsLoops = eventsLoops > 0 ? eventsLoops : Runtime.getRuntime().availableProcessors();
	}

	private static class ReactorEntry {
		final Reactor reactor;
		final Long    created;

		private ReactorEntry(Reactor reactor) {
			this.reactor = reactor;
			this.created = java.lang.System.nanoTime();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ReactorEntry that = (ReactorEntry) o;
			return reactor.equals(that.reactor);

		}

		@Override
		public int hashCode() {
			return reactor.hashCode();
		}
	}
}
