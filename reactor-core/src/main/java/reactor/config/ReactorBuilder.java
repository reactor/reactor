package reactor.config;

import reactor.core.Reactor;
import reactor.fn.dispatch.*;

/**
 * @author Stephane Maldini
 */
@SuppressWarnings("unchecked")
public abstract class ReactorBuilder<BUILDER extends ReactorBuilder<BUILDER, TARGET>, TARGET> {

	protected Dispatcher dispatcher;
	protected Reactor    reactor;

	public BUILDER using(Reactor reactor) {
		this.reactor = reactor;
		return (BUILDER) this;
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

	public BUILDER using(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		return (BUILDER) this;
	}

	public BUILDER sync() {
		this.dispatcher = new SynchronousDispatcher();
		return (BUILDER) this;
	}

	public BUILDER threadPoolExecutor() {
		this.dispatcher = new ThreadPoolExecutorDispatcher();
		return (BUILDER) this;
	}

	public BUILDER eventLoop() {
		this.dispatcher = new BlockingQueueDispatcher();
		return (BUILDER) this;
	}

	public BUILDER ringBuffer() {
		this.dispatcher = new RingBufferDispatcher();
		return (BUILDER) this;
	}

	public BUILDER dispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		return (BUILDER) this;
	}

	public TARGET build() {
		return doBuild(configureReactor());
	}

	public abstract TARGET doBuild(final Reactor reactor);
}
