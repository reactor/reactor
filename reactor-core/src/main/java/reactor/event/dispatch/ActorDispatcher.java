package reactor.event.dispatch;

import reactor.event.Event;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.routing.EventRouter;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.util.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link Dispatcher} that maps a key to a delegate dispatcher and caches the mapping within
 * its internal {@link Registry<Dispatcher>}. Thus making similar key-based dispatching reusing the same dispatcher,
 * a pattern also dubbed as "Actor".
 *
 * @author Stephane Maldini
 */
public class ActorDispatcher implements Dispatcher {

	private final Function<Object,Dispatcher> delegateMapper;
	private final Registry<Dispatcher> dispatcherCache = new CachingRegistry<Dispatcher>();

	public ActorDispatcher(Function<Object,Dispatcher> delegate) {
		Assert.notNull(delegate, "Delegate Dispatcher Supplier cannot be null.");
		this.delegateMapper = delegate;
	}

	@Override
	public boolean alive() {
		boolean alive = true;
		for(Registration<? extends Dispatcher> dispatcherRegistration : dispatcherCache){
			alive &= dispatcherRegistration.getObject().alive();
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
		for(Registration<? extends Dispatcher> dispatcherRegistration : dispatcherCache){
			alive &= dispatcherRegistration.getObject().awaitAndShutdown(timeout, timeUnit);
			if(!alive) break;
		}
		return alive;
	}

	@Override
	public void shutdown() {
		for(Registration<? extends Dispatcher> dispatcherRegistration : dispatcherCache){
			dispatcherRegistration.getObject().shutdown();
		}
	}

	@Override
	public void halt() {
		for(Registration<? extends Dispatcher> dispatcherRegistration : dispatcherCache){
			dispatcherRegistration.getObject().halt();
		}
	}

	@Override
	public <E extends Event<?>> void dispatch(Object key,
	                                          E event,
	                                          Registry<Consumer<? extends Event<?>>> consumerRegistry,
	                                          Consumer<Throwable> errorConsumer,
	                                          EventRouter eventRouter,
	                                          Consumer<E> completionConsumer) {

		List<Registration<? extends Dispatcher>> dispatchers = dispatcherCache.select(key);
		Dispatcher delegate;
		if(!dispatchers.isEmpty()){
			delegate = dispatchers.get(0).getObject();
		}else{
			delegate = delegateMapper.apply(key);
			dispatcherCache.register(Selectors.$(key), delegate);
		}

		delegate.dispatch(
				key,
				event,
				consumerRegistry,
				errorConsumer,
				eventRouter,
				completionConsumer);
	}

	@Override
	public <E extends Event<?>> void dispatch(E event, EventRouter eventRouter, Consumer<E> consumer, Consumer<Throwable> errorConsumer) {
		dispatch(null, event, null, errorConsumer, eventRouter, consumer);
	}

}
