package reactor.core.dynamic.reflect;

import static reactor.Fn.$;
import static reactor.core.dynamic.reflect.support.MethodNameUtils.methodNameToSelectorName;

import java.lang.reflect.Method;

import reactor.core.dynamic.annotation.On;
import reactor.core.dynamic.reflect.support.AnnotationUtils;
import reactor.fn.Selector;

/**
 * An implementation of {@link MethodSelectorResolver} that looks for an {@link On} annotation
 * or uses the method name minus the "on" portion and lower-casing the first character.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public class SimpleMethodSelectorResolver implements MethodSelectorResolver {

	@Override
	public Selector apply(Method method) {
		String sel;
		On onAnno = AnnotationUtils.find(method, On.class);
		if (null != onAnno) {
			sel = onAnno.value();
		} else {
			sel = methodNameToSelectorName(method.getName());
		}

		return (!"".equals(sel) ? $(sel) : null);
	}

	@Override
	public boolean supports(Method method) {
		return method.getDeclaringClass() != Object.class && !method.getName().contains("$");
	}



}
