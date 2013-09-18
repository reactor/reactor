package reactor.core.support;

import reactor.core.Environment;
import reactor.function.Supplier;

/**
 * A default implementation of a {@link reactor.function.Supplier} that provides a singleton {@link reactor.core
 * .Environment} which is created in the constructor.
 *
 * @author Jon Brisbin
 */
public class DefaultEnvironmentSupplier implements Supplier<Environment> {

	private final Environment env;

	public DefaultEnvironmentSupplier() {
		this.env = new Environment();
	}

	@Override
	public Environment get() {
		return env;
	}

}
