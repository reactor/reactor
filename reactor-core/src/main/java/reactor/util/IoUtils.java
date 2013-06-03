package reactor.util;

import java.io.Closeable;
import java.io.IOException;

/**
 * IO-related utility methods
 *
 * @author Andy Wilkinson
 *
 */
public final class IoUtils {

	private IoUtils() {

	}

	/**
	 * {@link Closeable#close Closes} each of the {@code closeables} swallowing any
	 * {@link IOException IOExceptions} that are thrown.
	 *
	 * @param closeables to be closed
	 */
	public static void closeQuietly(Closeable... closeables) {
		for (Closeable closeable: closeables) {
			if (closeable != null) {
				try {
					closeable.close();
				} catch (IOException ioe) {
					// Closing quietly.
				}
			}
		}
	}
}
