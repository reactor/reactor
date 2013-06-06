package reactor.tcp.encoding;

import reactor.fn.Event;
import reactor.fn.Function;
import reactor.fn.Observable;
import reactor.io.Buffer;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

/**
 * @author Jon Brisbin
 */
public class StringCodec implements Codec<Buffer, String, String> {

	private final Charset utf8 = Charset.forName("UTF-8");

	@Override
	public Function<Buffer, String> decoder(Object notifyKey, Observable observable) {
		return new StringDecoder(notifyKey, observable);
	}

	@Override
	public Function<String, Buffer> encoder() {
		return new StringEncoder();
	}

	private class StringDecoder implements Function<Buffer, String> {
		private final Object     notifyKey;
		private final Observable observable;
		private final CharsetDecoder decoder = utf8.newDecoder();

		private StringDecoder(Object notifyKey, Observable observable) {
			this.notifyKey = notifyKey;
			this.observable = observable;
		}

		@Override
		public String apply(Buffer bytes) {
			try {
				String s = decoder.decode(bytes.byteBuffer()).toString();
				if (null != notifyKey && null != observable) {
					observable.notify(notifyKey, Event.wrap(s));
					return null;
				} else {
					return s;
				}
			} catch (CharacterCodingException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	private class StringEncoder implements Function<String, Buffer> {
		private final CharsetEncoder encoder = utf8.newEncoder();

		@Override
		public Buffer apply(String s) {
			try {
				ByteBuffer bb = encoder.encode(CharBuffer.wrap(s));
				return new Buffer(bb);
			} catch (CharacterCodingException e) {
				throw new IllegalStateException(e);
			}
		}
	}

}
