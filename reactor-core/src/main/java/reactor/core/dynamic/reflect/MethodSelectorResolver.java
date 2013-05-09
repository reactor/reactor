package reactor.core.dynamic.reflect;

import reactor.fn.Function;
import reactor.fn.Selector;
import reactor.fn.Supports;

import java.lang.reflect.Method;

/**
 * @author Jon Brisbin
 */
public interface MethodSelectorResolver extends Supports<Method>, Function<Method, Selector> {
}
