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

package reactor.io.net;

import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Publisher;
import reactor.Publishers;
import reactor.Timers;
import reactor.core.support.Assert;
import reactor.fn.Function;
import reactor.fn.timer.Timer;

/**
 * Abstract base class that implements common functionality shared by clients and servers.
 * <p> A Peer is network component with start and shutdown capabilities. On Start it will
 * require a {@link ReactiveChannelHandler} to process the incoming {@link
 * ReactiveChannel}, regardless of being a server or a client.
 * @author Stephane Maldini
 */
public abstract class ReactivePeer<IN, OUT, CONN extends ReactiveChannel<IN, OUT>> {

	private final   Timer         defaultTimer;
	private final   long          defaultPrefetch;
	protected final AtomicBoolean started;

	protected ReactivePeer(Timer defaultTimer) {
		this(defaultTimer, Long.MAX_VALUE);
	}

	protected ReactivePeer(Timer defaultTimer, long prefetch) {
		this.defaultTimer = defaultTimer == null && Timers.hasGlobal() ? Timers.global() :
				defaultTimer;
		this.defaultPrefetch = prefetch > 0 ? prefetch : Long.MAX_VALUE;
		this.started = new AtomicBoolean();
	}

	/**
	 * Start this {@literal Peer}.
	 * @return a {@link Publisher<Void>} that will be complete when the {@link
	 * ReactivePeer} is started
	 */
	public final Publisher<Void> start(
			final ReactiveChannelHandler<IN, OUT, CONN> handler) {

		if (!started.compareAndSet(false, true) && shouldFailOnStarted()) {
			throw new IllegalStateException("Peer already started");
		}

		return doStart(handler);
	}

	/**
	 * @see this#start(ReactiveChannelHandler)
	 * @param handler
	 * @throws InterruptedException
	 */
	public final void startAndAwait(final ReactiveChannelHandler<IN, OUT, CONN> handler)
			throws InterruptedException {
		Publishers.toReadQueue(start(handler)).take();
	}

	/**
	 * Shutdown this {@literal Peer} and complete the returned {@link Publisher<Void>}
	 * when shut down.
	 * @return a {@link Publisher<Void>} that will be complete when the {@link
	 * ReactivePeer} is shutdown
	 */
	public final Publisher<Void> shutdown() {
		if (started.compareAndSet(true, false)) {
			return doShutdown();
		}
		return Publishers.empty();
	}

	/**
	 * @see this#shutdown()
	 * @throws InterruptedException
	 */
	public final void shutdownAndAwait()
			throws InterruptedException {
		Publishers.toReadQueue(shutdown()).take();
	}

	/**
	 *
	 * @param preprocessor
	 * @param <NEWIN>
	 * @param <NEWOUT>
	 * @param <NEWCONN>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final <NEWIN, NEWOUT, NEWCONN extends ReactiveChannel<NEWIN, NEWOUT>, P extends ReactivePeer<NEWIN, NEWOUT, NEWCONN>> P preprocessor(
			final Function<? super CONN, NEWCONN> preprocessor){

		checkPreprocessor(preprocessor);

		return (P)doPreprocessor(preprocessor);
	}


	/* Implementation Contract */

	/**
	 *
	 * @param preprocessor
	 * @param <NEWIN>
	 * @param <NEWOUT>
	 * @return
	 */
	protected <NEWIN, NEWOUT> ReactivePeer<NEWIN, NEWOUT, ReactiveChannel<NEWIN, NEWOUT>> doPreprocessor(
			final Function<? super CONN, ? extends ReactiveChannel<NEWIN, NEWOUT>> preprocessor
	){
		return new PreprocessedReactivePeer<>(preprocessor);
	}

	protected final void checkPreprocessor(
			Function<?, ?> preprocessor) {
		Assert.isTrue(preprocessor != null, "Preprocessor argument must be provided");

		if (started.get()) {
			throw new IllegalStateException("Peer already started");
		}
	}

	/**
	 * Get the default environment for all Channel
	 * @return The default environment
	 */
	public final Timer getDefaultTimer() {
		return defaultTimer;
	}

	/**
	 * Get the default batch read/write size
	 * @return the default capacity, default Long.MAX for unbounded
	 */
	public final long getDefaultPrefetchSize() {
		return defaultPrefetch;
	}

	protected abstract Publisher<Void> doStart(
			ReactiveChannelHandler<IN, OUT, CONN> handler);

	protected abstract Publisher<Void> doShutdown();

	protected boolean shouldFailOnStarted() {
		return true;
	}

	private final class PreprocessedReactivePeer<NEWIN, NEWOUT, NEWCONN extends ReactiveChannel<NEWIN, NEWOUT>>
			extends ReactivePeer<NEWIN, NEWOUT, NEWCONN> {

		private final Function<? super CONN, ? extends NEWCONN> preprocessor;

		public PreprocessedReactivePeer(Function<? super CONN, ? extends NEWCONN> preprocessor) {
			super(ReactivePeer.this.defaultTimer, ReactivePeer.this.defaultPrefetch);
			this.preprocessor = preprocessor;
		}

		@Override
		protected Publisher<Void> doStart(
				final ReactiveChannelHandler<NEWIN, NEWOUT, NEWCONN> handler) {
			ReactiveChannelHandler<IN, OUT, CONN> p = Preprocessor.PreprocessedHandler.create(handler, preprocessor);
			return ReactivePeer.this.start(p);
		}

		@Override
		protected Publisher<Void> doShutdown() {
			return ReactivePeer.this.shutdown();
		}
	}

}
