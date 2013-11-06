package reactor.spring.annotation;

import org.springframework.stereotype.Component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Denotes a class as being eligible to scan for event handling methods. Composite annotation that includes the {@link
 * org.springframework.stereotype.Component} annotation.
 *
 * @author Jon Brisbin
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Component
public @interface Consumer {
}
