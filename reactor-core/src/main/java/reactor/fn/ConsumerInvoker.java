package reactor.fn;

import reactor.convert.Converter;

/**
 * Implementations of this interface are responsible for invoking a {@link Consumer} that may take into account
 * automatic argument conversion, return values, and other situations that might be specific to a particular use-case.
 *
 * @author Jon Brisbin
 */
public interface ConsumerInvoker extends Supports<Consumer<?>> {

	/**
	 * Invoke a {@link Consumer}.
	 *
	 * @param consumer     The {@link Consumer} to invoke.
	 * @param converter    The {@link Converter} to be aware of. May be {@literal null}.
	 * @param returnType   If the {@link Consumer} also implements a value-returning type, convert it to this type before
	 *                     returning.
	 * @param possibleArgs An array of possible arguments that may or may not be used.
	 * @param <T>          The return type.
	 * @return A result if available, or {@literal null} otherwise.
	 * @throws Exception
	 */
	<T> T invoke(Consumer<?> consumer,
							 Converter converter,
							 Class<? extends T> returnType,
							 Object... possibleArgs) throws Exception;

}
