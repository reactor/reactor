package reactor.core.dynamic.reflect;

import java.lang.reflect.Method;

import reactor.fn.Function;
import reactor.fn.Supports;

/**
 * When given a {@link Method}, a {@code MethodNotificationKeyResolver} will attempt to return
 * a notification key for the method.
 *
 * @author Andy Wilkinson
 *
 */
public interface MethodNotificationKeyResolver extends Supports<Method>, Function<Method, String> {

}
