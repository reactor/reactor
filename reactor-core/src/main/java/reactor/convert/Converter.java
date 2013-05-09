package reactor.convert;

/**
 * @author Stephane Maldini (smaldini)
 * @date: 3/21/13
 *
 * Port from Spring org.springframework.core.convert.ConversionService
 */
public interface Converter {

	/**
	 * Returns true if objects of sourceType can be converted to targetType.
	 * If this method returns true, it means {@link #convert(Object, Class)} is capable of converting an instance of sourceType to targetType.
	 * @param sourceType the source type to convert from (may be null if source is null)
	 * @param targetType the target type to convert to (required)
	 * @return true if a conversion can be performed, false if not
	 */
	boolean canConvert(Class<?> sourceType, Class<?> targetType);

	/**
	 * Convert the source to targetType.
	 * @param source the source object to convert (may be null)
	 * @param targetType the target type to convert to (required)
	 * @return the converted object, an instance of targetType
	 */
	<T> T convert(Object source, Class<T> targetType);
}
