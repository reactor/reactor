package reactor.config;

import reactor.core.Reactor;
import reactor.fn.dispatch.*;

/**
 * @author Stephane Maldini
 */
@SuppressWarnings("unchecked")
public abstract class ReactorBuilder<B extends ReactorBuilder<B, X>, X> {

	protected Dispatcher dispatcher;
	protected Reactor    reactor;

	public B using(Reactor reactor) {
		this.reactor = reactor;
		return (B) this;
	}

	protected Reactor configureReactor() {
		Reactor reactor;
		if (null == this.reactor) {
			if (null == dispatcher) {
				reactor = new Reactor();
			} else {
				reactor = new Reactor(dispatcher);
			}
		} else {
			reactor = new Reactor(this.reactor);
		}
		return reactor;
	}

	public B using(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		return (B) this;
	}

	public B sync() {
		this.dispatcher = new SynchronousDispatcher();
		return (B) this;
	}

	public B threadPoolExecutor() {
		this.dispatcher = new ThreadPoolExecutorDispatcher();
		return (B) this;
	}

	public B eventLoop() {
		this.dispatcher = new BlockingQueueDispatcher();
		return (B) this;
	}

	public B ringBuffer() {
		this.dispatcher = new RingBufferDispatcher();
		return (B) this;
	}

	public B dispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		return (B) this;
	}

	public X build() {
		return doBuild(configureReactor());
	}

	public abstract X doBuild(final Reactor reactor);
}
