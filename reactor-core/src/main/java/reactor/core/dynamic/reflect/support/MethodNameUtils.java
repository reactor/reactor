package reactor.core.dynamic.reflect.support;

/**
 * @author Jon Brisbin
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
		String s = name.replaceAll("([A-Z])", ".$1").toLowerCase();
		if (s.startsWith("on")) {
			return s.replaceFirst("^on\\.", "");
		} else if (s.startsWith("notify")) {
			return s.replaceFirst("^notify\\.", "");
		}
		return null;
	}

}
