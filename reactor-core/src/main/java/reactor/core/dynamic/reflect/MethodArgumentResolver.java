package reactor.core.dynamic.reflect;

import reactor.fn.Function;
import reactor.fn.Supports;

/**
 * @author Jon Brisbin
 */
public interface MethodArgumentResolver<T> extends Supports<Class<?>>, Function<Class<T>, T> {
}
