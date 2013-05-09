package reactor.spring.context.annotation;

import java.lang.annotation.*;

/**
 * @author Jon Brisbin
 */
@Target({
						ElementType.TYPE,
						ElementType.METHOD
				})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface On {

	/**
	 * An expression that evaluates to the {@link reactor.core.Reactor} on which to place this handler.
	 *
	 * @return An expression to be evaluated.
	 */
	String reactor() default "";

	/**
	 * An expression that evaluates to a {@link reactor.fn.Selector} to register this handler with the {@link
	 * reactor.core.Reactor}.
	 *
	 * @return An expression to be evaluated.
	 */
	String selector() default "";

}
