package reactor.spring.dynamic;

import org.springframework.beans.factory.FactoryBean;
import reactor.core.dynamic.DynamicReactor;
import reactor.core.dynamic.DynamicReactorFactory;

/**
 * @author Jon Brisbin
 */
public class DynamicReactorFactoryBean<T extends DynamicReactor> implements FactoryBean<T> {

	private final boolean                  singleton;
	private final Class<T>                 type;
	private final DynamicReactorFactory<T> reactorFactory;

	public DynamicReactorFactoryBean(Class<T> type) {
		this.singleton = false;
		this.type = type;
		this.reactorFactory = new DynamicReactorFactory<T>(type);
	}

	public DynamicReactorFactoryBean(Class<T> type, boolean singleton) {
		this.singleton = singleton;
		this.type = type;
		this.reactorFactory = new DynamicReactorFactory<T>(type);
	}

	@Override
	public T getObject() throws Exception {
		return reactorFactory.create();
	}

	@Override
	public Class<?> getObjectType() {
		return type;
	}

	@Override
	public boolean isSingleton() {
		return singleton;
	}

}
