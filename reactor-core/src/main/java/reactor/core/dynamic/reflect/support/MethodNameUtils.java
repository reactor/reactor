package reactor.core.dynamic.reflect.support;

/**
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public abstract class MethodNameUtils {

	protected MethodNameUtils() {
	}

	/**
	 * Strip the "on" or "notify" from a method name and split the camel-case into a dot-separated {@literal String}.
	 *
	 * @param name The method name to transform.
	 * @return A camel-case-to-dot-separated version suitable for a {@link reactor.fn.Selector}.
	 */
	public static String methodNameToSelectorName(String name) {
		return doTransformation(name, "on", "^on\\.");
	}

	/**
	 * Strip the "notify" from a method name and split the camel-case into a dot-separated {@literal String}.
	 *
	 * @param name The method name to transform.
	 * @return A camel-case-to-dot-separated version suitable for a notification key
	 */
	public static String methodNameToNotificationKey(String name) {
		return doTransformation(name, "notify", "^notify\\.");
	}

	private static String doTransformation(String name, String prefix, String replacementRegex) {
		String s = name.replaceAll("([A-Z])", ".$1").toLowerCase();
		if (s.startsWith(prefix)) {
			return s.replaceFirst(replacementRegex, "");
		} else {
			return null;
		}
	}

}
