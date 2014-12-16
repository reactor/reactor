/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.core.support;

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
