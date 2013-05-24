package reactor.core;

import reactor.convert.Converter;
import reactor.convert.DelegatingConverter;
import reactor.fn.Registry;
import reactor.fn.SelectionStrategy;
import reactor.fn.Supplier;
import reactor.fn.TagAwareSelectionStrategy;
import reactor.fn.dispatch.*;

import java.util.List;

/**
 * @author Stephane Maldini
 */
@SuppressWarnings("unchecked")
public abstract class ReactorBuilder<BUILDER extends ReactorBuilder<BUILDER, TARGET>,
		TARGET> implements Supplier<TARGET> {

	protected Dispatcher                     dispatcher;
	protected Reactor                        reactor;
	protected Converter                      converter;
	protected Registry.LoadBalancingStrategy loadBalancingStrategy;
	protected SelectionStrategy              selectionStrategy;

	protected boolean share = false;

	public BUILDER using(Reactor reactor) {
		this.reactor = reactor;
		return (BUILDER) this;
	}

	protected Reactor configureReactor() {
		final Reactor _reactor;
		if (null == reactor) {
			_reactor = new Reactor(dispatcher, loadBalancingStrategy, selectionStrategy, converter);
		} else if (share) {
			_reactor = reactor;
		} else {
			_reactor = new Reactor(
					null == dispatcher ? reactor.getDispatcher() : dispatcher,
					null == loadBalancingStrategy ? reactor.getConsumerRegistry().getLoadBalancingStrategy() : loadBalancingStrategy,
					null == selectionStrategy ? reactor.getConsumerRegistry().getSelectionStrategy() : selectionStrategy,
					null == converter ? reactor.getConverter() : converter
			);
		}
		return _reactor;
	}

	public BUILDER share() {
		this.share = true;
		return (BUILDER) this;
	}

	public BUILDER using(Converter converter) {
		this.converter = converter;
		return (BUILDER) this;
	}

	public BUILDER using(SelectionStrategy selectionStrategy) {
		this.selectionStrategy = selectionStrategy;
		return (BUILDER) this;
	}

	public BUILDER pubSub(){
		this.loadBalancingStrategy = Registry.LoadBalancingStrategy.NONE;
		return (BUILDER) this;
	}

	public BUILDER randomDistribution(){
		this.loadBalancingStrategy = Registry.LoadBalancingStrategy.RANDOM;
		return (BUILDER) this;
	}

	public BUILDER p2p(){
		this.loadBalancingStrategy = Registry.LoadBalancingStrategy.ROUND_ROBIN;
		return (BUILDER) this;
	}

	public BUILDER tagFiltering(){
		this.selectionStrategy = new TagAwareSelectionStrategy();
		return (BUILDER) this;
	}

	public BUILDER using(Converter... converters){
		this.converter = new DelegatingConverter(converters);
		return (BUILDER) this;
	}

	public BUILDER using(List<Converter> converters) {
		this.converter = new DelegatingConverter(converters);
		return (BUILDER) this;
	}

	public BUILDER using(Registry.LoadBalancingStrategy loadBalancingStrategy) {
		this.loadBalancingStrategy = loadBalancingStrategy;
		return (BUILDER) this;
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
		this.dispatcher = new ThreadPoolExecutorDispatcher().start();
		return (BUILDER) this;
	}

	public BUILDER eventLoop() {
		this.dispatcher = new BlockingQueueDispatcher().start();
		return (BUILDER) this;
	}

	public BUILDER ringBuffer() {
		this.dispatcher = new RingBufferDispatcher().start();
		return (BUILDER) this;
	}

	public BUILDER dispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		return (BUILDER) this;
	}

	public TARGET get() {
		return doBuild(configureReactor());
	}

	protected abstract TARGET doBuild(final Reactor reactor);
}
