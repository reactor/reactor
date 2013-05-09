package reactor.spring.context;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionService;
import reactor.core.R;
import reactor.core.Reactor;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ReactorFactoryBean implements FactoryBean<Reactor> {

	private static final Reactor ROOT_REACTOR = new Reactor();

	static {
		R.link(ROOT_REACTOR);
	}

	@Autowired(required = false)
	private ConversionService conversionService;
	private boolean rootReactor = false;
	private String name;


	public ReactorFactoryBean(boolean rootReactor) {
		this.rootReactor = rootReactor;
	}

	public ReactorFactoryBean() {
	}

	public boolean isRootReactor() {
		return rootReactor;
	}

	public ReactorFactoryBean setRootReactor(boolean rootReactor) {
		this.rootReactor = rootReactor;
		return this;
	}

	public String getName() {
		return name;
	}

	public ReactorFactoryBean setName(String name) {
		this.name = name;
		return this;
	}

	public ConversionService getConversionService() {
		return conversionService;
	}

	public ReactorFactoryBean setConversionService(ConversionService conversionService) {
		this.conversionService = conversionService;
		return this;
	}

	@Override
	public Reactor getObject() throws Exception {
		Reactor reactor;
		if (rootReactor) {
			reactor = ROOT_REACTOR;
		} else if (null != name) {
			reactor = R.createOrGet(name);
		} else {
			reactor = R.create();
		}

		if (conversionService != null) {
			reactor.setConverter(new ConversionServiceConverter(conversionService));
		}

		return reactor;
	}

	@Override
	public Class<?> getObjectType() {
		return Reactor.class;
	}

	@Override
	public boolean isSingleton() {
		return rootReactor || null != name;
	}

}
