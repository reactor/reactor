package reactor.support;

/**
 * @author Stephane Maldini (smaldini)
 * @date: 3/21/13
 *
 * Port the assertion logic from spring-beans to avoid dependency
 */
public class Assert {

	public static void notNull(Object object, String message){
		if (object == null) {
			throw new IllegalArgumentException(message);
		}
	}

	public static void isTrue(boolean expression, String message) {
		if (!expression) {
			throw new IllegalArgumentException(message);
		}
	}
}
