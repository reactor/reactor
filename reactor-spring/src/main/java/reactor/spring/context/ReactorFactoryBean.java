package reactor.spring.context;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.util.Assert;
import reactor.R;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.spring.context.EventRouting;

/**
 * @author Jon Brisbin
 */
public class ReactorFactoryBean implements FactoryBean<Reactor> {

	private final    Environment env;
	private volatile Reactor     reactor;

	public ReactorFactoryBean(Environment env) {
		this(env, null, null);
	}

	public ReactorFactoryBean(Environment env,
														String dispatcher) {
		this(env, dispatcher, null);
	}

	public ReactorFactoryBean(Environment env,
														String dispatcher,
														EventRouting eventRouting) {
		Assert.notNull(env, "Environment cannot be null.");
		this.env = env;

		Reactor.Spec spec = R.reactor().using(env);
		if (null != dispatcher) {
			spec.dispatcher(dispatcher);
		}
		if (null != eventRouting) {
			switch (eventRouting) {
				case BROADCAST_EVENT_ROUTING:
					spec.broadcastEventRouting();
					break;
				case RANDOM_EVENT_ROUTING:
					spec.randomEventRouting();
					break;
				case ROUND_ROBIN_EVENT_ROUTING:
					spec.roundRobinEventRouting();
					break;
			}
		}
		this.reactor = spec.get();
	}

	@Override
	public Reactor getObject() throws Exception {
		return reactor;
	}

	@Override
	public Class<?> getObjectType() {
		return Reactor.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

}
