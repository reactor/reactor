package reactor.spring.context.config;

import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Helper annotation to be placed on {@link org.springframework.context.annotation.Configuration} classes to ensure
 * an {@link reactor.core.Environment} is created in application context as well as create the necessary beans for
 * automatic wiring of annotated beans.
 *
 * @author Jon Brisbin
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(ReactorBeanDefinitionRegistrar.class)
public @interface EnableReactor {

	/**
	 * Name of the profile to use as the default.
	 *
	 * @return default profile name
	 */
	String value() default "";

	/**
	 * The bean name of {@link reactor.function.Supplier} that can provide an instance (or instances) of {@link reactor
	 * .core.Environment} to be registered in the {@link org.springframework.context.ApplicationContext}.
	 *
	 * @return bean name of {@link reactor.core.Environment} {@link reactor.function.Supplier}
	 */
	String environmentSupplier() default "";

}
