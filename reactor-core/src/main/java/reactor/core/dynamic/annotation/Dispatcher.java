package reactor.core.dynamic.annotation;

import java.lang.annotation.*;

/**
 * @author Jon Brisbin
 */
@Target({
						ElementType.TYPE
				})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Dispatcher {

	DispatcherType value() default DispatcherType.WORKER;

}
