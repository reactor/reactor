package reactor.core.dynamic.reflect.support;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * Utility methods for working with Annotations.
 *
 * @author Andy Wilkinson
 * @author Jon Brisbin
 *
 */
public final class AnnotationUtils {

	private AnnotationUtils() {

	}

	/**
	 * Finds the annotation of the given {@code type} on the given {@code method}.
	 *
	 * @param m The Method to find the annotation an
	 * @param type The type of annotation to find
	 *
	 * @return The annotation that was found, or {@code null}.
	 */
	@SuppressWarnings("unchecked")
	public static <A extends Annotation> A find(Method m, Class<A> type) {
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
