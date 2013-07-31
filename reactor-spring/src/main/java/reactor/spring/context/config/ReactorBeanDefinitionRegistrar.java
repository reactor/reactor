package reactor.spring.context.config;

import java.util.Map;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.StringUtils;
import reactor.core.Environment;
import reactor.core.configuration.PropertiesConfigurationReader;
import reactor.spring.beans.factory.config.ConsumerBeanPostProcessor;

/**
 * {@link ImportBeanDefinitionRegistrar} implementation that configures necessary Reactor components.
 *
 * @author Jon Brisbin
 */
public class ReactorBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar {
	@Override
	public void registerBeanDefinitions(AnnotationMetadata meta, BeanDefinitionRegistry registry) {
		Map<String, Object> attrs = meta.getAnnotationAttributes(EnableReactor.class.getName());

		// Create a root Enivronment
		if(!registry.containsBeanDefinition(Environment.class.getName())) {
			BeanDefinitionBuilder envBeanDef = BeanDefinitionBuilder.rootBeanDefinition(Environment.class);

			String configReaderBean = (String)attrs.get("configurationReader");
			if(StringUtils.hasText(configReaderBean)) {
				envBeanDef.addConstructorArgReference(configReaderBean);
			} else {
				String profileName = (String)attrs.get("value");
				if(StringUtils.hasText(profileName)) {
					envBeanDef.addConstructorArgValue(new PropertiesConfigurationReader(profileName));
				}
			}
			registry.registerBeanDefinition(Environment.class.getName(), envBeanDef.getBeanDefinition());
		}

		// Create a ConsumerBeanPostProcessor
		if(!registry.containsBeanDefinition(ConsumerBeanPostProcessor.class.getName())) {
			BeanDefinitionBuilder envBeanDef = BeanDefinitionBuilder.rootBeanDefinition(ConsumerBeanPostProcessor.class);
			registry.registerBeanDefinition(ConsumerBeanPostProcessor.class.getName(), envBeanDef.getBeanDefinition());
		}
	}
}
