/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A simple circuit-breaker domain representation for accumulating successes and failures then decide if the "circuit"
 * is open after a crossing a threshold. Observing open/close state is achieved with
 * {@link Streams#circuitBreaker(org.reactivestreams.Publisher, org.reactivestreams.Publisher, reactor.rx.CircuitBreaker}
 *
 * @author Stephane Maldini
 */
public class CircuitBreaker implements Serializable {

	private final long     timespan;
	private final TimeUnit unit;
	private final int      size;

	private volatile int successes;
	private volatile int fails;


	protected static final AtomicIntegerFieldUpdater<CircuitBreaker> TERMINAL_UPDATER = AtomicIntegerFieldUpdater
			.newUpdater(CircuitBreaker.class, "successes");


	public static CircuitBreaker create(long timespan, TimeUnit unit, int size) {
		return new CircuitBreaker(timespan, unit, size);
	}

	protected CircuitBreaker(long timespan, TimeUnit unit, int size) {
		this.timespan = timespan;
		this.unit = unit;
		this.size = size;
	}

	public boolean incrementSuccess() {
		return false;
	}
}
