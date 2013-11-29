package reactor.io.encoding;

import reactor.function.Consumer;
import reactor.function.Function;

/**
 * A simple {@code Codec} that uses the source object as both input and output. Override the {@link
 * #beforeAccept(Object)} and {@link #beforeApply(Object)} methods to intercept data coming in and going out
 * (respectively).
 *
 * @author Jon Brisbin
 */
public class PassThroughCodec<SRC> implements Codec<SRC, SRC, SRC> {
	@Override
	public Function<SRC, SRC> decoder(final Consumer<SRC> next) {
		return new Function<SRC, SRC>() {
			@Override
			public SRC apply(SRC src) {
				if(null != next) {
					next.accept(beforeAccept(src));
					return null;
				} else {
					return beforeAccept(src);
				}
			}
		};
	}

	@Override
	public Function<SRC, SRC> encoder() {
		return new Function<SRC, SRC>() {
			@Override
			public SRC apply(SRC src) {
				return beforeApply(src);
			}
		};
	}

	/**
	 * Override to intercept the source object before it is passed into the next {@link reactor.function.Consumer} or
	 * returned to the caller if a {@link reactor.function.Consumer} is not set.
	 *
	 * @param src The source object.
	 *
	 * @return
	 */
	protected SRC beforeAccept(SRC src) {
		// NO-OP
		return src;
	}

	/**
	 * Override to intercept the source object before it is returned for output.
	 *
	 * @param src
	 *
	 * @return
	 */
	protected SRC beforeApply(SRC src) {
		// NO-OP
		return src;
	}

}
