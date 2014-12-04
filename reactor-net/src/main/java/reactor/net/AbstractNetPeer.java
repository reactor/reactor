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

package reactor.net;

import com.gs.collections.impl.list.mutable.FastList;
import reactor.core.Environment;
import reactor.event.Event;
import reactor.event.EventBus;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;

/**
 * Abstract base class that implements common functionality shared by clients and servers.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class AbstractNetPeer<IN, OUT> {

	private final Registry<NetChannel<IN, OUT>>   netChannels = new CachingRegistry<NetChannel<IN, OUT>>();
	private final Event<AbstractNetPeer<IN, OUT>> selfEvent   = Event.wrap(this);
	private final Selector                        open        = Selectors.$();
	private final Selector                        close       = Selectors.$();
	private final Selector                        start       = Selectors.$();
	private final Selector                        shutdown    = Selectors.$();

	private final Environment                               env;
	private final EventBus                                  reactor;
	private final Codec<Buffer, IN, OUT>                    codec;
	private final Collection<Consumer<NetChannel<IN, OUT>>> consumers;

	protected AbstractNetPeer(@Nonnull Environment env,
	                          @Nonnull EventBus reactor,
	                          @Nullable Codec<Buffer, IN, OUT> codec,
	                          @Nonnull Collection<Consumer<NetChannel<IN, OUT>>> consumers) {
		this.env = env;
		this.reactor = reactor;
		this.codec = codec;
		this.consumers = consumers;

		for (final Consumer<NetChannel<IN, OUT>> consumer : consumers) {
			reactor.on(open, new Consumer<Event<NetChannel<IN, OUT>>>() {
				@Override
				public void accept(Event<NetChannel<IN, OUT>> ev) {
					consumer.accept(ev.getData());
				}
			});
		}
	}

	public Promise<Boolean> close() {
		Promise<Boolean> d = Promises.ready(env, reactor.getDispatcher());
		close(d);
		return d;
	}

	public void close(@Nullable final Consumer<Boolean> onClose) {
		for (Registration<? extends NetChannel<IN, OUT>> reg : getChannels()) {
			if (!reg.isCancelled()) {
				doCloseChannel(reg.getObject());
			}
		}
		if (null != onClose) {
			reactor.schedule(onClose, true);
		}
	}

	public Iterator<NetChannel<IN, OUT>> iterator() {
		FastList<NetChannel<IN, OUT>> channels = FastList.newList();
		for (Registration<? extends NetChannel<IN, OUT>> reg : getChannels()) {
			channels.add(reg.getObject());
		}
		return channels.iterator();
	}

	/**
	 * Subclasses should register the given {@link reactor.net.NetChannel} for later use.
	 *
	 * @param ioChannel
	 * 		The channel object.
	 * @param netChannel
	 * 		The {@link NetChannel}.
	 * @param <C>
	 * 		The type of the channel object.
	 *
	 * @return {@link reactor.event.registry.Registration} of this channel in the {@link Registry}.
	 */
	protected <C> Registration<? extends NetChannel<IN, OUT>> register(@Nonnull C ioChannel,
	                                                                   @Nonnull NetChannel<IN, OUT> netChannel) {
		Assert.notNull(ioChannel, "Channel cannot be null.");
		Assert.notNull(netChannel, "NetChannel cannot be null.");
		return netChannels.register(Selectors.$(ioChannel), netChannel);
	}

	/**
	 * Find the {@link NetChannel} for the given IO channel object.
	 *
	 * @param ioChannel
	 * 		The channel object.
	 * @param <C>
	 * 		The type of the channel object.
	 *
	 * @return The {@link NetChannel} associated with the given channel.
	 */
	protected <C> NetChannel<IN, OUT> select(@Nonnull C ioChannel) {
		Assert.notNull(ioChannel, "Channel cannot be null.");
		Iterator<Registration<? extends NetChannel<IN, OUT>>> channs = netChannels.select(ioChannel).iterator();
		if (channs.hasNext()) {
			return channs.next().getObject();
		} else {
			NetChannel<IN, OUT> conn = createChannel(ioChannel);
			register(ioChannel, conn);
			notifyOpen(conn);
			return conn;
		}
	}

	/**
	 * Close the given channel.
	 *
	 * @param channel
	 * 		The channel object.
	 * @param <C>
	 * 		The type of the channel object.
	 */
	protected <C> void close(@Nonnull C channel) {
		Assert.notNull(channel, "Channel cannot be null");
		for (Registration<? extends NetChannel<IN, OUT>> reg : netChannels.select(channel)) {
			NetChannel<IN, OUT> chann = reg.getObject();
			reg.cancel();
			notifyClose(chann);
		}
	}

	/**
	 * Subclasses should implement this method and provide a {@link NetChannel} object.
	 *
	 * @param ioChannel
	 * 		The IO channel object to associate with this {@link reactor.net.NetChannel}.
	 * @param <C>
	 * 		The type of the channel object.
	 *
	 * @return The new {@link NetChannel} object.
	 */
	protected abstract <C> NetChannel<IN, OUT> createChannel(C ioChannel);

	/**
	 * Notify this server's consumers that the server has started.
	 */
	protected void notifyStart(final Runnable started) {
		getReactor().notify(start.getObject(), selfEvent);
		if (null != started) {
			getReactor().schedule(new Consumer<Runnable>() {
				@Override
				public void accept(Runnable r) {
					r.run();
				}
			}, started);
		}
	}

	/**
	 * Notify this client's consumers than a global error has occurred.
	 *
	 * @param error
	 * 		The error to notify.
	 */
	protected void notifyError(@Nonnull Throwable error) {
		Assert.notNull(error, "Error cannot be null.");
		reactor.notify(error.getClass(), Event.wrap(error));
	}

	/**
	 * Notify this peer's consumers that the channel has been opened.
	 *
	 * @param channel
	 * 		The channel that was opened.
	 */
	protected void notifyOpen(@Nonnull NetChannel<IN, OUT> channel) {
		reactor.notify(open.getObject(), Event.wrap(channel));
	}

	/**
	 * Notify this peer's consumers that the given channel has been closed.
	 *
	 * @param channel
	 * 		The channel that was closed.
	 */
	protected void notifyClose(@Nonnull NetChannel<IN, OUT> channel) {
		reactor.notify(close.getObject(), Event.wrap(channel));
	}

	/**
	 * Notify this server's consumers that the server has stopped.
	 */
	protected void notifyShutdown() {
		getReactor().notify(shutdown.getObject(), selfEvent);
	}

	/**
	 * Get the {@link Codec} in use.
	 *
	 * @return The codec. May be {@literal null}.
	 */
	@Nullable
	protected Codec<Buffer, IN, OUT> getCodec() {
		return codec;
	}

	@Nonnull
	protected Environment getEnvironment() {
		return env;
	}

	@Nonnull
	protected EventBus getReactor() {
		return reactor;
	}

	@Nonnull
	protected Collection<Consumer<NetChannel<IN, OUT>>> getConsumers() {
		return consumers;
	}

	@Nonnull
	protected Registry<NetChannel<IN, OUT>> getChannels() {
		return netChannels;
	}

	/**
	 * Subclasses should implement this method to perform the actual IO channel close.
	 *
	 * @param onClose
	 */
	protected void doClose(@Nullable Consumer<Boolean> onClose) {
		getReactor().schedule(onClose, true);
	}

	/**
	 * Close the given channel.
	 *
	 * @param channel
	 */
	protected void doCloseChannel(NetChannel<IN, OUT> channel) {
		channel.close();
	}

}
