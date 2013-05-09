package reactor.core.dynamic.reflect;

import reactor.core.dynamic.annotation.Notify;
import reactor.core.dynamic.annotation.On;
import reactor.fn.Selector;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static reactor.Fn.$;
import static reactor.core.dynamic.reflect.support.MethodNameUtils.methodNameToSelectorName;

/**
 * An implementation of {@link MethodSelectorResolver} that looks for either an {@link On} or {@link Notify} annotation
 * or uses the method name minus the "on" or "notify" portion and lower-casing the first character.
 *
 * @author Jon Brisbin
 */
public class SimpleMethodSelectorResolver implements MethodSelectorResolver {

	@Override
	public Selector apply(Method method) {
		String sel;
		On onAnno = find(method, On.class);
		if (null != onAnno) {
			sel = onAnno.value();
		} else {
			Notify notifyAnno = find(method, Notify.class);
			if (null != notifyAnno) {
				sel = notifyAnno.value();
			} else {
				sel = methodNameToSelectorName(method.getName());
			}
		}
		return (!"".equals(sel) ? $(sel) : $());
	}

	@Override
	public boolean supports(Method method) {
		return method.getDeclaringClass() != Object.class && !method.getName().contains("$");
	}

	@SuppressWarnings("unchecked")
	private static <A extends Annotation> A find(Method m, Class<A> type) {
		if (m.getDeclaredAnnotations().length > 0) {
			for (Annotation anno : m.getDeclaredAnnotations()) {
				if (type.isAssignableFrom(anno.getClass())) {
					return (A) anno;
				}
			}
		}
		return null;
	}

}
