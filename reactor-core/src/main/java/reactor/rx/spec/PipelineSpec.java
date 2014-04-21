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
package reactor.rx.spec;

import reactor.core.Environment;
import reactor.core.spec.support.DispatcherComponentSpec;
import reactor.event.dispatch.Dispatcher;

/**
 * A helper class for specifying a bounded {@link reactor.rx.Stream}.
 *
 * @param <SPEC>   The ComposableSpec subclass
 * @param <TARGET> The type that this spec will create
 * @author Stephane Maldini
 */
public abstract class PipelineSpec<SPEC extends PipelineSpec<SPEC, TARGET>, TARGET> extends DispatcherComponentSpec<SPEC,
		TARGET> {

	@Override
	protected TARGET configure(final Dispatcher dispatcher, Environment env) {
		return createPipeline(env, dispatcher);
	}

	protected abstract TARGET createPipeline(Environment env, Dispatcher dispatcher);

}
