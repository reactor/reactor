package reactor.core.dynamic.annotation;

import java.lang.annotation.*;

/**
 * Annotation to denote that a method should proxy a call to an underlying {@link reactor.core.Reactor#notify(Object, reactor.fn.Event))}.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
@Target({
						ElementType.METHOD
				})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Notify {

	/**
	 * The string to use as a key.
	 *
	 * @return
	 */
	String value() default "";

}
