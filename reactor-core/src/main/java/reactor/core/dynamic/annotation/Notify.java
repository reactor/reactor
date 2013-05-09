package reactor.core.dynamic.annotation;

import java.lang.annotation.*;

/**
 * Annotation to denote that a method should proxy a call to an underlying {@link reactor.core.Reactor#notify(reactor.fn.Selector,
 * reactor.fn.Event)}.
 *
 * @author Jon Brisbin
 */
@Target({
						ElementType.METHOD
				})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Notify {

	/**
	 * The string to use as a {@link reactor.fn.Selector}.
	 *
	 * @return
	 */
	String value() default "";

}
