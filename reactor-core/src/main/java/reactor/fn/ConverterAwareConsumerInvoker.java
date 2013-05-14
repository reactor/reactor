package reactor.fn;

import reactor.convert.Converter;

import java.util.concurrent.Callable;

/**
 * This implementation of a {@link ConsumerInvoker} will attempt to invoke a {@link Consumer} as-is and, if that fails
 * with a {@link ClassCastException} because the argument declared in the {@literal Consumer} isn't of the correct type,
 * it tries to find an object of that type in the array of {@literal possibleArgs} passed to the invoker. If that fails,
 * it will attempt to use the given {@link Converter} to convert the argument into a form acceptable to the {@literal
 * Consumer}. If the argument is of type {@link Event} and the data inside that event is of a compatible type with the
 * argument to the consumer, this invoker will unwrap that {@literal Event} and try to invoke the consumer using the
 * data itself.
 * <p/>
 * Finally, if the {@literal Consumer} also implements {@link Callable}, then it will invoke the {@link
 * java.util.concurrent.Callable#call()} method to obtain a return value and return that. Otherwise it will return
 * {@literal null} or throw any raised exceptions.
 *
 * @author Jon Brisbin
 */
public class ConverterAwareConsumerInvoker implements ConsumerInvoker {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public <T> T invoke(Consumer<?> consumer,
											Converter converter,
											Class<? extends T> returnType,
											Object... possibleArgs) throws Exception {
		try {
			((Consumer) consumer).accept((possibleArgs.length > 0 ? possibleArgs[0] : null));
		} catch (ClassCastException e) {
			Class<?> argType = ConsumerUtils.resolveArgType(consumer);
			if (argType == Object.class) {
				throw e;
			}
			for (int i = 1; i < possibleArgs.length; i++) {
				Object o = possibleArgs[i];
				if (null == o) {
					continue;
				}
				if (argType.isAssignableFrom(o.getClass())) {
					// arg type matches a possible arg
					return invoke(consumer, converter, returnType, o);
				} else if (null != converter && converter.canConvert(o.getClass(), argType)) {
					return invoke(consumer, converter, returnType, converter.convert(o, argType));
				} else if (Event.class.isAssignableFrom(o.getClass())
						&& null != ((Event<?>) o).getData()
						&& argType.isAssignableFrom(((Event<?>) o).getData().getClass())) {
					return invoke(consumer, converter, returnType, ((Event<?>) o).getData());
				}
			}
			throw e;
		}

		if (Void.TYPE == returnType) {
			return null;
		}

		if (consumer instanceof Callable) {
			Object o = ((Callable<Object>) consumer).call();

			if (null == o) {
				return null;
			}

			if (returnType.isAssignableFrom(o.getClass())) {
				return (T) o;
			} else if (null != converter && converter.canConvert(o.getClass(), returnType)) {
				return converter.convert(o, returnType);
			} else {
				throw new IllegalArgumentException("Cannot convert object of type " + o.getClass().getName() + " to " + returnType.getName());
			}
		}
		return null;
	}

	@Override
	public boolean supports(Consumer<?> consumer) {
		return true;
	}

}
