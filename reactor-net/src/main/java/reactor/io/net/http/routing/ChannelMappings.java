/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.io.net.http.routing;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.reactivestreams.Publisher;
import reactor.core.publisher.convert.DependencyUtils;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.http.HttpChannel;

/**
 * @author Stephane Maldini
 */
public abstract class ChannelMappings<IN, OUT>
		implements Function<HttpChannel<IN, OUT>, Iterable<? extends ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>>>{

	private static final boolean FORCE_SIMPLE_MAPPINGS =
			Boolean.parseBoolean(System.getProperty("reactor.net.forceSimpleMappings", "false"));

	/**
	 *
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> ChannelMappings<IN, OUT> newMappings(){
		if(DependencyUtils.hasReactorBus() && !FORCE_SIMPLE_MAPPINGS){
			return new RegistryChannelMappings<>();
		}
		else{
			return new SimpleChannelMappings<>();
		}
	}

	/**
	 *
	 * @param condition
	 * @param handler
	 * @return
	 */
	public abstract ChannelMappings<IN, OUT> add(Predicate<? super HttpChannel<IN, OUT>> condition,
			ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler);


	/**
	 *
	 * @param <IN>
	 * @param <OUT>
	 */
	public static final class HttpHandlerMapping<IN, OUT>
			implements ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>,
			           Predicate<HttpChannel<IN, OUT>>{

		private final Predicate<? super HttpChannel<IN, OUT>>               condition;
		private final ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler;
		private final Function<? super String, Map<String, Object>>         resolver;

		HttpHandlerMapping(Predicate<? super HttpChannel<IN, OUT>> condition,
				ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler,
				Function<? super String, Map<String, Object>>         resolver) {
			this.condition = condition;
			this.handler = handler;
			this.resolver = resolver;
		}

		@Override
		public Publisher<Void> apply(HttpChannel<IN, OUT> channel) {
			return handler.apply(channel.paramsResolver(resolver));
		}

		@Override
		public boolean test(HttpChannel<IN, OUT> o) {
			return condition.test(o);
		}

		/**
		 *
		 * @return
		 */
		public Predicate<? super HttpChannel<IN, OUT>> getCondition() {
			return condition;
		}

		/**
		 *
		 * @return
		 */
		public ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> getHandler() {
			return handler;
		}
	}


	private static class SimpleChannelMappings<IN, OUT> extends ChannelMappings<IN, OUT>{

		private final CopyOnWriteArrayList<HttpHandlerMapping<IN, OUT>> handlers =
				new CopyOnWriteArrayList<>();

		@Override
		public Iterable<? extends ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>> apply(
				final HttpChannel<IN, OUT> channel) {

			return new Iterable<ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>>() {
				@Override
				public Iterator<ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>> iterator() {
					final Iterator<HttpHandlerMapping<IN, OUT>> iterator = handlers.iterator();
					return new Iterator<ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>>>() {

						HttpHandlerMapping<IN, OUT> cached;

						@Override
						public boolean hasNext() {
							HttpHandlerMapping<IN, OUT> cursor;
							while( iterator.hasNext() ){
								cursor = iterator.next();
								if(cursor.test(channel)){
									cached = cursor;
									return true;
								}
							}
							return false;
						}

						@Override
						public ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> next() {
							HttpHandlerMapping<IN, OUT> cursor = cached;
							if(cursor != null){
								cached = null;
								return cursor;
							}
							hasNext();
							return cached;
						}
					};
				}
			};
		}

		@Override
		public ChannelMappings<IN, OUT> add(Predicate<? super HttpChannel<IN, OUT>> condition,
				ReactiveChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {

			handlers.add(new HttpHandlerMapping<>(condition, handler, null));
			return this;
		}
	}
}
