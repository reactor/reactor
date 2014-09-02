package reactor.groovy.config;


import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.event.Event;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.routing.ConsumerFilteringRouter;
import reactor.event.routing.ConsumerInvoker;
import reactor.filter.Filter;
import reactor.function.Consumer;

import java.util.List;

/**
 * @author Stephane Maldini
 */
public class StreamRouter extends ConsumerFilteringRouter {

	private final Registry<Processor<Event<?>, Event<?>>> processorRegistry;

	public StreamRouter(Filter filter, ConsumerInvoker consumerInvoker,
	                    Registry<Processor<Event<?>, Event<?>>> processorRegistry) {
		super(filter, consumerInvoker);
		this.processorRegistry = processorRegistry;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <E> void route(final Object key, final E event,
	                      final List<Registration<? extends Consumer<?>>> consumers,
	                      final Consumer<E> completionConsumer,
	                      final Consumer<Throwable> errorConsumer) {

		Processor<Event<?>, Event<?>> processor;
		for (Registration<? extends Processor<Event<?>, Event<?>>> registration : processorRegistry.select(key)){
			processor = registration.getObject();
			processor.onNext((Event<?>) event);
			processor.subscribe(new Subscriber<Event<?>>() {
				@Override
				public void onSubscribe(Subscription subscription) {
					subscription.request(Integer.MAX_VALUE);
				}

				@Override
				public void onNext(Event<?> hydratedEvent) {
					StreamRouter.super.route(hydratedEvent.getKey(), (E) hydratedEvent, consumers, completionConsumer,
							errorConsumer);
				}

				@Override
				public void onComplete() {

				}

				@Override
				public void onError(Throwable cause) {
					((Event<?>) event).consumeError(cause);
				}
			});
		}
	}

}
