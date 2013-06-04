package reactor.fn.dispatch;

import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.registry.Registry;
import reactor.fn.routing.EventRouter;

abstract class BaseDispatcher implements Dispatcher {

	@Override
	public <E extends Event<?>> void dispatch(Object key, E event, Registry<Consumer<? extends Event<?>>> consumerRegistry, Consumer<Throwable> errorConsumer, EventRouter eventRouter, Consumer<E> completionConsumer) {
		if (!alive()) {
			throw new IllegalStateException("This Dispatcher has been shutdown");
		}

		Task<E> task = createTask();

		task.setKey(key);
		task.setEvent(event);
		task.setConsumerRegistry(consumerRegistry);
		task.setErrorConsumer(errorConsumer);
		task.setEventRouter(eventRouter);
		task.setCompletionConsumer(completionConsumer);

		task.submit();
	}

	protected abstract <E extends Event<?>> Task<E> createTask();

	protected abstract class Task<E extends Event<?>> {

		private volatile Object                                 key;
		private volatile Registry<Consumer<? extends Event<?>>> consumerRegistry;
		private volatile E                                      event;
		private volatile Consumer<E>                            completionConsumer;
		private volatile Consumer<Throwable>                    errorConsumer;
		private volatile EventRouter                            eventRouter;

		public Task<E> setKey(Object key) {
			this.key = key;
			return this;
		}

		public Task<E> setConsumerRegistry(Registry<Consumer<? extends Event<?>>> consumerRegistry) {
			this.consumerRegistry = consumerRegistry;
			return this;
		}

		public Task<E> setEvent(E event) {
			this.event = event;
			return this;
		}

		public Task<E> setCompletionConsumer(Consumer<E> completionConsumer) {
			this.completionConsumer = completionConsumer;
			return this;
		}

		public Task<E> setErrorConsumer(Consumer<Throwable> errorConsumer) {
			this.errorConsumer = errorConsumer;
			return this;
		}

		public Task<E> setEventRouter(EventRouter eventRouter) {
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
