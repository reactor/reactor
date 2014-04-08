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
public class TraceableDelegatingRouter implements Router {

	private final Router delegate;
	private final Logger log;

	public TraceableDelegatingRouter(Router delegate) {
		Assert.notNull(delegate, "Delegate EventRouter cannot be null.");
		this.delegate = delegate;
		this.log = LoggerFactory.getLogger(delegate.getClass());
	}

	@Override
	public void route(Object key,
	                  Object event,
	                  List<Registration<? extends Consumer<?>>> consumers,
	                  Consumer<?> completionConsumer,
	                  Consumer<Throwable> errorConsumer) {
		if(log.isTraceEnabled()) {
			log.trace("route({}, {}, {}, {}, {})", key, event, consumers, completionConsumer, errorConsumer);
		}
		delegate.route(key,event,consumers,completionConsumer,errorConsumer);
	}

}
