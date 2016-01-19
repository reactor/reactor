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

package reactor.bus.spec;

import org.reactivestreams.Processor;
import reactor.core.publisher.ProcessorGroup;
import reactor.fn.Supplier;

/**
 * A generic processor-aware class for specifying components that need to be configured
 * with an {@link Processor}.
 *
 * @param <SPEC>   The ProcessorComponentSpec subclass spec extensions
 * @param <TARGET> The type that this spec will create
 * @param <PAYLOAD> The type that will transit into the underneath {@link Processor}
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
@SuppressWarnings("unchecked")
public abstract class ProcessorComponentSpec<SPEC extends ProcessorComponentSpec<SPEC, TARGET, PAYLOAD>, TARGET, PAYLOAD>
  implements Supplier<TARGET> {


	private Processor<PAYLOAD, PAYLOAD>   processor;

	private int concurrency = 1;

	/**
	 * Configures the component to use a synchronous processor
	 *
	 * @return {@code this}
	 */
	@SuppressWarnings("unchecked")
	public final SPEC sync() {
		this.processor =(Processor<PAYLOAD, PAYLOAD>) ProcessorGroup.sync().get();
		return (SPEC) this;
	}

	/**
	 * Configures the component to use the given {@code dispatcher}
	 *
	 * @param processor The dispatcher to use
	 * @return {@code this}
	 */
	public final SPEC processor(Processor<PAYLOAD, PAYLOAD> processor) {
		this.processor = processor;
		return (SPEC) this;
	}

	/**
	 * Configures the component to use the given {@code concurrency} (e.g., number of concurrent threads for EventBus)
	 *
	 * @param concurrency The concurrency hint for the component resulting in matching number of processor.subscribe()
	 * @return {@code this}
	 */
	public final SPEC concurrency(int concurrency) {
		this.concurrency = concurrency;
		return (SPEC) this;
	}

	@Override
	public final TARGET get() {
		return configure(getProcessor(), concurrency);
	}

	@SuppressWarnings("unchecked")
	private Processor<PAYLOAD, PAYLOAD> getProcessor() {
		if (this.processor != null) {
			return this.processor;
		} else {
			return (Processor<PAYLOAD, PAYLOAD>) ProcessorGroup.sync().get();
		}
	}

	protected abstract TARGET configure(Processor<PAYLOAD, PAYLOAD> processor, int concurrency);

}
