/*
 * Copyright (c) 2011-2013 the original author or authors.
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

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class to encapsulate commonly-used functionality around Reactors and build a clean context.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class R {


	private final int eventsLoops;
	private final AtomicLong                               nextDispatcherCounter = new AtomicLong(Long.MIN_VALUE);
	private final NonBlockingHashMap<String, ReactorEntry> reactors              = new NonBlockingHashMap<String,
			ReactorEntry>();

	public R() {
		this(Runtime.getRuntime().availableProcessors());
	}

	public R(int eventsLoops) {
		this.eventsLoops = eventsLoops > 0 ? eventsLoops : Runtime.getRuntime().availableProcessors();
	}

	private static class ReactorEntry {
		final Reactor reactor;
		final Long    created;

		private ReactorEntry(Reactor reactor) {
			this.reactor = reactor;
			this.created = System.nanoTime();
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


