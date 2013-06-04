package reactor.fn.dispatch;

import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.registry.Registry;
import reactor.fn.routing.EventRouter;

abstract class BaseDispatcher implements Dispatcher {

	@Override
	public <T, E extends Event<T>> void dispatch(Object key, E event, Registry<Consumer<? extends Event<?>>> consumerRegistry, Consumer<Throwable> errorConsumer, EventRouter eventRouter, Consumer<E> completionConsumer) {
		if (!alive()) {
			throw new IllegalStateException("This Dispatcher has been shutdown");
		}

		Task<T, E> task = createTask();

		task.setKey(key);
		task.setEvent(event);
		task.setConsumerRegistry(consumerRegistry);
		task.setErrorConsumer(errorConsumer);
		task.setEventRouter(eventRouter);
		task.setCompletionConsumer(completionConsumer);

		task.submit();
	}

	protected abstract <T, E extends Event<T>> Task<T, E> createTask();

	protected abstract class Task<T, E extends Event<T>> {

		private volatile Object                                 key;
		private volatile Registry<Consumer<? extends Event<?>>> consumerRegistry;
		private volatile Event<T>                               event;
		private volatile Consumer<E>                            completionConsumer;
		private volatile Consumer<Throwable>                    errorConsumer;
		private volatile EventRouter                            eventRouter;

		public Task<T, E> setKey(Object key) {
			this.key = key;
			return this;
		}

		public Task<T, E> setConsumerRegistry(Registry<Consumer<? extends Event<?>>> consumerRegistry) {
			this.consumerRegistry = consumerRegistry;
			return this;
		}

		public Task<T, E> setEvent(Event<T> event) {
			this.event = event;
			return this;
		}

		public Task<T, E> setCompletionConsumer(Consumer<E> completionConsumer) {
			this.completionConsumer = completionConsumer;
			return this;
		}

		public Task<T, E> setErrorConsumer(Consumer<Throwable> errorConsumer) {
			this.errorConsumer = errorConsumer;
			return this;
		}

		public Task<T, E> setEventRouter(EventRouter eventRouter) {
			this.eventRouter = eventRouter;
			return this;
		}

		public void reset() {
			key = null;
			consumerRegistry = null;
			event = null;
			completionConsumer = null;
			errorConsumer = null;
		}

		public abstract void submit();

		protected void execute() {
			eventRouter.route(key, event, consumerRegistry.select(key), completionConsumer, errorConsumer);
		}
	}
}
