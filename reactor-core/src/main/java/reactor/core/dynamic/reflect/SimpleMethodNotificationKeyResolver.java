package reactor.core.dynamic.reflect;

import static reactor.core.dynamic.reflect.support.MethodNameUtils.methodNameToNotificationKey;

import java.lang.reflect.Method;

import reactor.core.dynamic.annotation.Notify;
import reactor.core.dynamic.reflect.support.AnnotationUtils;

/**
 * An implementation of {@link MethodNotificationKeyResolver} that looks for an {@link Notify} annotation
 * or uses the method name minus the "notify" portion and lower-casing the first character.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public final class SimpleMethodNotificationKeyResolver implements MethodNotificationKeyResolver {

	@Override
	public boolean supports(Method method) {
		return method.getDeclaringClass() != Object.class && !method.getName().contains("$");
	}

	@Override
	public String apply(Method method) {
		String key;
		Notify notifyAnno = AnnotationUtils.find(method, Notify.class);

		if (null != notifyAnno) {
			key = notifyAnno.value();
		} else {
			key = methodNameToNotificationKey(method.getName());
		}

		return key;
	}
}
