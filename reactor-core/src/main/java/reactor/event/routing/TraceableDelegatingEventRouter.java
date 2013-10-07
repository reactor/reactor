package reactor.event.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.event.Event;
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.util.Assert;

import java.util.List;

/**
 * @author Jon Brisbin
 */
public class TraceableDelegatingEventRouter implements EventRouter {

	private final EventRouter delegate;
	private final Logger      log;

	public TraceableDelegatingEventRouter(EventRouter delegate) {
		Assert.notNull(delegate, "Delegate EventRouter cannot be null.");
		this.delegate = delegate;
		this.log = LoggerFactory.getLogger(delegate.getClass());
	}

	@Override
	public void route(Object key,
	                  Event<?> event,
	                  List<Registration<? extends Consumer<? extends Event<?>>>> consumers,
	                  Consumer<?> completionConsumer,
	                  Consumer<Throwable> errorConsumer) {
		if(log.isTraceEnabled()) {
			log.trace("route({}, {}, {}, {}, {})");
		}
	}

}
