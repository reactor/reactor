package reactor.spring.context.config;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.StringUtils;
import reactor.core.Environment;
import reactor.core.configuration.PropertiesConfigurationReader;
import reactor.spring.beans.factory.config.ConsumerBeanAutoConfiguration;

import java.util.Map;

/**
 * {@link ImportBeanDefinitionRegistrar} implementation that configures necessary Reactor components.
 *
 * @author Jon Brisbin
 */
public class ReactorBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar {

	private static final String DEFAULT_ENV_NAME = "reactorEnv";

	@Override
	public void registerBeanDefinitions(AnnotationMetadata meta, BeanDefinitionRegistry registry) {
		Map<String, Object> attrs = meta.getAnnotationAttributes(EnableReactor.class.getName());

		// Create a root Enivronment
		if (!registry.containsBeanDefinition(DEFAULT_ENV_NAME)) {
			BeanDefinitionBuilder envBeanDef = BeanDefinitionBuilder.rootBeanDefinition(Environment.class);

			String configReaderBean = (String) attrs.get("configurationReader");
			if (StringUtils.hasText(configReaderBean)) {
				envBeanDef.addConstructorArgReference(configReaderBean);
			} else {
				String profileName = (String) attrs.get("value");
				if (StringUtils.hasText(profileName)) {
					envBeanDef.addConstructorArgValue(new PropertiesConfigurationReader(profileName));
				}
			}
			registry.registerBeanDefinition(DEFAULT_ENV_NAME, envBeanDef.getBeanDefinition());
		}

		// Create a ConsumerBeanAutoConfiguration
		if (!registry.containsBeanDefinition(ConsumerBeanAutoConfiguration.class.getName())) {
			BeanDefinitionBuilder autoConfigDef = BeanDefinitionBuilder.rootBeanDefinition(ConsumerBeanAutoConfiguration.class);
			autoConfigDef.addConstructorArgReference(DEFAULT_ENV_NAME);
			registry.registerBeanDefinition(ConsumerBeanAutoConfiguration.class.getName(), autoConfigDef.getBeanDefinition());
		}
	}
}
