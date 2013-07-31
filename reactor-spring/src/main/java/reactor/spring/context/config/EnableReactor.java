package reactor.spring.context.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;

/**
 * @author Jon Brisbin
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(ReactorBeanDefinitionRegistrar.class)
public @interface EnableReactor {

	/**
	 * Name of the profile to use as the default.
	 *
	 * @return
	 */
	String value() default "";

	/**
	 * The name of a {@link reactor.core.configuration.ConfigurationReader} bean to use to read the Reactor {@link
	 * reactor.core.Environment}.
	 *
	 * @return
	 */
	String configurationReader() default "";

}
