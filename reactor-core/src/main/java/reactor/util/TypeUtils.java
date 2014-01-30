package reactor.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * @author Jon Brisbin
 */
public abstract class TypeUtils {

	protected TypeUtils() {
	}

	public static <T> Type fromGenericType(Class<T> type) {
		return ((ParameterizedType)type.getGenericInterfaces()[0]).getActualTypeArguments()[0];
	}

	public static <T> Type fromTypeRef(TypeReference<T> typeRef) {
		return fromGenericType(typeRef.getClass());
	}

}
