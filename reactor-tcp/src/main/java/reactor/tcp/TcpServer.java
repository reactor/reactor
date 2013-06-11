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

package reactor.tcp;

import reactor.core.ComponentSpec;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.registry.Registration;
import reactor.fn.registry.CachingRegistry;
import reactor.fn.registry.Registry;
import reactor.fn.selector.Selector;
import reactor.fn.tuples.Tuple2;
import reactor.io.Buffer;
import reactor.tcp.encoding.Codec;
import reactor.util.Assert;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static reactor.Fn.$;

/**
 * @author Jon Brisbin
 */
public abstract class TcpServer<IN, OUT> {

	private final Event<TcpServer<IN, OUT>>        selfEvent   = Event.wrap(this);
	private final Tuple2<Selector, Object>         start       = $();
	private final Tuple2<Selector, Object>         shutdown    = $();
	private final Tuple2<Selector, Object>         open        = $();
	private final Tuple2<Selector, Object>         close       = $();
	private final Registry<TcpConnection<IN, OUT>> connections = new CachingRegistry<TcpConnection<IN, OUT>>(null);

	private final Reactor                reactor;
	private final Codec<Buffer, IN, OUT> codec;

	protected final Environment env;

	protected TcpServer(Environment env,
											Reactor reactor,
											InetSocketAddress listenAddress,
											int backlog,
											int rcvbuf,
											int sndbuf,
											Codec<Buffer, IN, OUT> codec,
											Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers) {
		Assert.notNull(env, "A TcpServer cannot be created without a properly-configured Environment.");
		Assert.notNull(reactor, "A TcpServer cannot be created without a properly-configured Reactor.");
		this.env = env;
		this.reactor = reactor;
		this.codec = codec;

		for (final Consumer<TcpConnection<IN, OUT>> consumer : connectionConsumers) {
			this.reactor.on(open.getT1(), new Consumer<Event<TcpConnection<IN, OUT>>>() {
				@Override
				public void accept(Event<TcpConnection<IN, OUT>> ev) {
					consumer.accept(ev.getData());
				}
			});
		}
	}

	public TcpServer<IN, OUT> start() {
		return start(null);
	}

	public abstract TcpServer<IN, OUT> start(Consumer<Void> started);

	public TcpServer<IN, OUT> shutdown() {
		return shutdown(null);
	}

	public abstract TcpServer<IN, OUT> shutdown(Consumer<Void> stopped);

	protected <C> Registration<? extends TcpConnection<IN, OUT>> register(C channel, TcpConnection<IN, OUT> connection) {
		return connections.register($(channel), connection);
	}

	protected <C> TcpConnection<IN, OUT> select(C channel) {
		Iterator<Registration<? extends TcpConnection<IN, OUT>>> conns = connections.select(channel).iterator();
		if (conns.hasNext()) {
			return conns.next().getObject();
		} else {
			TcpConnection<IN, OUT> conn = createConnection(channel);
			register(channel, conn);
			notifyOpen(conn);
			return conn;
		}
	}

	protected <C> void close(C channel) {
		for (Registration<? extends TcpConnection<IN, OUT>> reg : connections.select(channel)) {
			TcpConnection<IN, OUT> conn = reg.getObject();
			reg.getObject().close();
			notifyClose(conn);
			reg.cancel();
		}
	}

	protected abstract <C> TcpConnection<IN, OUT> createConnection(C channel);

	protected void notifyError(Throwable error) {
		reactor.notify(error.getClass(), Event.wrap(error));
	}

	protected void notifyStart(final Consumer<Void> started) {
		if (null != started) {
			reactor.on(start.getT1(), new Consumer<Event<Void>>() {
				@Override
				public void accept(Event<Void> ev) {
					started.accept(null);
				}
			});
		}
		reactor.notify(start.getT2(), selfEvent);
	}

	protected void notifyShutdown(final Consumer<Void> stopped) {
		if (null != stopped) {
			reactor.on(shutdown.getT1(), new Consumer<Event<Void>>() {
				@Override
				public void accept(Event<Void> ev) {
					stopped.accept(null);
				}
			});
		}
		reactor.notify(shutdown.getT2(), selfEvent);
	}

	protected void notifyOpen(TcpConnection<IN, OUT> conn) {
		reactor.notify(open.getT2(), Event.wrap(conn));
	}

	protected void notifyClose(TcpConnection<IN, OUT> conn) {
		reactor.notify(close.getT2(), Event.wrap(conn));
	}

	protected Codec<Buffer, IN, OUT> getCodec() {
		return codec;
	}

	public static class Spec<IN, OUT> extends ComponentSpec<Spec<IN, OUT>, TcpServer<IN, OUT>> {
		private final Class<? extends TcpServer<IN, OUT>>       serverImpl;
		private final Constructor<? extends TcpServer<IN, OUT>> serverImplConstructor;

		private InetSocketAddress listenAddress;
		private int backlog = 512;
		private int rcvbuf  = 8 * 1024;
		private int sndbuf  = 8 * 1024;
		private Codec<Buffer, IN, OUT>                       codec;
		private Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers;

		@SuppressWarnings({"rawtypes"})
		public Spec(Class<? extends TcpServer> serverImpl,
								Codec<Buffer, IN, OUT> codec) {
			this(serverImpl);
			this.codec = codec;
		}

		@SuppressWarnings({"unchecked", "rawtypes"})
		public Spec(Class<? extends TcpServer> serverImpl) {
			Assert.notNull(serverImpl, "TcpServer implementation class cannot be null.");
			this.serverImpl = (Class<? extends TcpServer<IN, OUT>>) serverImpl;
			try {
				this.serverImplConstructor = (Constructor<? extends TcpServer<IN, OUT>>) serverImpl.getDeclaredConstructor(
						Environment.class,
						Reactor.class,
						InetSocketAddress.class,
						int.class,
						int.class,
						int.class,
						Codec.class,
						Collection.class
				);
				this.serverImplConstructor.setAccessible(true);
			} catch (NoSuchMethodException e) {
				throw new IllegalArgumentException("No public constructor found that matches the signature of the one found in the TcpServer class.");
			}
		}

		public Spec<IN, OUT> backlog(int backlog) {
			this.backlog = backlog;
			return this;
		}

		public Spec<IN, OUT> bufferSize(int rcvbuf, int sndbuf) {
			this.rcvbuf = rcvbuf;
			this.sndbuf = sndbuf;
			return this;
		}

		public Spec<IN, OUT> listen(int port) {
			Assert.isNull(listenAddress, "Listen address is already set.");
			this.listenAddress = new InetSocketAddress(port);
			return this;
		}

		public Spec<IN, OUT> listen(String host, int port) {
			Assert.isNull(listenAddress, "Listen address is already set.");
			this.listenAddress = new InetSocketAddress(host, port);
			return this;
		}

		public Spec<IN, OUT> codec(Codec<Buffer, IN, OUT> codec) {
			Assert.isNull(this.codec, "Codec has already been set.");
			this.codec = codec;
			return this;
		}

		public Spec<IN, OUT> consume(Consumer<TcpConnection<IN, OUT>> connectionConsumer) {
			return consume(Collections.singletonList(connectionConsumer));
		}

		public Spec<IN, OUT> consume(Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers) {
			this.connectionConsumers = connectionConsumers;
			return this;
		}

		@Override
		protected TcpServer<IN, OUT> configure(Reactor reactor) {
			try {
				return serverImplConstructor.newInstance(
						env,
						reactor,
						listenAddress,
						backlog,
						rcvbuf,
						sndbuf,
						codec,
						connectionConsumers
				);
			} catch (Throwable t) {
				throw new IllegalStateException(t);
			}
		}
	}

}
