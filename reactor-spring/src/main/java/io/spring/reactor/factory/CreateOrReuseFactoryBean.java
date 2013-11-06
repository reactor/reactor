package io.spring.reactor.factory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.*;
import reactor.function.Supplier;
import reactor.util.Assert;

/**
 * Spring {@link org.springframework.beans.factory.FactoryBean} implementation to provide either a new bean, created on
 * the first injection, or the previously-created bean thereafter.
 * <p>This is slightly different than letting the Spring container handle this behaviour as the instance will come from
 * the given {@link reactor.function.Supplier} the first time around.</p>
 *
 * @author Jon Brisbin
 */
public class CreateOrReuseFactoryBean<T> implements FactoryBean<T>,
                                                    BeanFactoryAware,
                                                    InitializingBean {

	private final Object monitor = new Object() {};
	private final String              self;
	private final Class<T>            type;
	private final Supplier<T>         supplier;
	private       ListableBeanFactory beanFactory;
	private       T                   instance;

	public CreateOrReuseFactoryBean(String self, Class<T> type, Supplier<T> supplier) {
		Assert.notNull(self, "'self' Bean name cannot be null.");
		Assert.notNull(type, "Bean type cannot be null.");
		Assert.notNull(supplier, "Supplier cannot be null.");
		this.self = self;
		this.type = type;
		this.supplier = supplier;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		if(beanFactory instanceof ListableBeanFactory) {
			this.beanFactory = (ListableBeanFactory)beanFactory;
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(beanFactory, "ListableBeanFactory cannot be null.");
	}

	@SuppressWarnings("unchecked")
	@Override
	public T getObject() throws Exception {
		synchronized(monitor) {
			if(null == instance) {
				String[] names = BeanFactoryUtils.beanNamesForTypeIncludingAncestors(beanFactory, type);
				if(names.length == 0 || names[0].equals(self)) {
					instance = supplier.get();
				} else {
					instance = (T)beanFactory.getBean(names[0]);
				}
			}
			return instance;
		}
	}

	@Override
	public Class<?> getObjectType() {
		return type;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

}
