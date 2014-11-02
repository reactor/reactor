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

package reactor.event.dispatch;

import reactor.event.registry.Registry;
import reactor.event.routing.Router;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.util.Assert;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link Dispatcher} that maps a key to a delegate dispatcher and caches the mapping within
 * its internal {@link Registry<Dispatcher>}. Thus making similar key-based dispatching reusing the same dispatcher,
 * a pattern also dubbed as "Actor".
 *
 * @author Stephane Maldini
 */
public final class ActorDispatcher implements Dispatcher {

	private final Function<Object, Dispatcher> delegateMapper;
	private final Map<Integer, Dispatcher> dispatcherCache = new ConcurrentHashMap<Integer, Dispatcher>();
	private final int                      emptyHashcode   = this.hashCode();

	public ActorDispatcher(Function<Object, Dispatcher> delegate) {
		Assert.notNull(delegate, "Delegate Dispatcher Supplier cannot be null.");
		this.delegateMapper = delegate;
	}

	@Override
	public boolean alive() {
		boolean alive = true;
		for(Dispatcher dispatcher : new HashSet<Dispatcher>(dispatcherCache.values())) {
			alive &= dispatcher.alive();
			if(!alive) break;
		}
		return alive;
	}

	@Override
	public boolean awaitAndShutdown() {
		return awaitAndShutdown(Integer.MAX_VALUE, TimeUnit.SECONDS);
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		boolean alive = true;
		for(Dispatcher dispatcher : new HashSet<Dispatcher>(dispatcherCache.values())) {
			if(dispatcher.alive()) {
				alive &= dispatcher.awaitAndShutdown(timeout, timeUnit);
			}
			if(!alive) break;
		}
		return alive;
	}

	@Override
	public void shutdown() {
		for(Dispatcher dispatcher : new HashSet<Dispatcher>(dispatcherCache.values())) {
			dispatcher.shutdown();
		}
	}

	@Override
	public void forceShutdown() {
		for(Dispatcher dispatcher : new HashSet<Dispatcher>(dispatcherCache.values())) {
			dispatcher.forceShutdown();
		}
	}

	@Override
	public <E> void dispatch(Object key,
	                                          E event,
	                                          Registry<Consumer<?>> consumerRegistry,
	                                          Consumer<Throwable> errorConsumer,
	                                          Router router,
	                                          Consumer<E> completionConsumer) {

		int hashCode = key == null ? emptyHashcode : key.hashCode();
		Dispatcher delegate = dispatcherCache.get(hashCode);
		if(delegate == null) {
			delegate = delegateMapper.apply(key);
			dispatcherCache.put(hashCode, delegate);
		}

		delegate.dispatch(
				key,
				event,
				consumerRegistry,
				errorConsumer,
				router,
				completionConsumer);
	}

	@Override
	public <E> void dispatch(E event,
	                                          Router router,
	                                          Consumer<E> consumer,
	                                          Consumer<Throwable> errorConsumer) {
		dispatch(null, event, null, errorConsumer, router, consumer);
	}

	@Override
	public long remainingSlots() {
		return Long.MAX_VALUE;
	}

	@Override
	public int backlogSize() {
		return Integer.MAX_VALUE;
	}

	@Override
	public boolean supportsOrdering() {
		return false;
	}

	@Override
	public void execute(Runnable command) {
		int hashCode = command.hashCode();
		Dispatcher delegate = dispatcherCache.get(hashCode);
		if(delegate == null) {
			delegate = delegateMapper.apply(command);
			dispatcherCache.put(hashCode, delegate);
		}

		delegate.execute(command);
	}

	@Override
	public boolean inContext() {
		return false;
	}
}
