/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.io.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

import reactor.convert.Converter;
import reactor.fn.Consumer;
import reactor.io.Buffer;

/**
 * A {@link Consumer} implementation that dumps everything it accepts to an open {@link
 * FileChannel}. If the accepted value is a {@link Buffer} the buffer's contents are
 * written to the channel. If a {@link Converter} is available, and the accepted type can
 * be converted to a {@code Buffer}, the value is converter and then written into the
 * channel. Otherwise, the accepted value is turned into a String by calling its {@code
 * toString} method, the string is wrapped in a buffer, and this buffer is written into
 * the channel.
 *
 * @param <T> The type of the values that can be accepted.
 *
 * @author Jon Brisbin
 */
public class FileChannelConsumer<T> implements Consumer<T> {

	private final File             outFile;
	private final FileOutputStream out;
	private final FileChannel      channel;
	private final AtomicLong position = new AtomicLong(0);
	private final Converter converter;

	/**
	 * Create a new timestamped file in the given directory, using the given basename. A timestamp will be append to the
	 * basename so no files will ever be overwritten using this {@link Consumer}. No conversion of accepted values
	 * will be performed.
	 *
	 * @param dir      The directory in which to create the file.
	 * @param basename The basename to use, to which will be appended a timestamp.
	 *
	 * @throws FileNotFoundException if the output file could not be created
	 */
	public FileChannelConsumer(String dir, String basename) throws FileNotFoundException {
		this(dir, basename, null);
	}

	/**
	 * Create a new timestamped file in the given directory, using the given basename. A timestamp will be append to the
	 * basename so no files will ever be overwritten using this {@link Consumer}.
	 *
	 * @param dir       The directory in which to create the file.
	 * @param basename  The basename to use, to which will be appended a timestamp.
	 * @param converter The {@link Converter} to use to turn accepted objects into {@link Buffer Buffers}.
	 *
	 * @throws FileNotFoundException if the output file could not be created
	 */
	public FileChannelConsumer(String dir, String basename, Converter converter) throws FileNotFoundException {
		this.converter = converter;

		File d = new File(dir);
		if (!d.exists()) {
			d.mkdirs();
		}

		String filename = basename + "-" + System.currentTimeMillis();
		this.outFile = new File(d, filename);
		this.out = new FileOutputStream(outFile);
		this.channel = out.getChannel();
	}

	/**
	 * Get the {@link File} object for the currently-open {@link FileChannel}.
	 *
	 * @return The {@link File} for this channel.
	 */
	public File getFile() {
		return outFile;
	}

	@Override
	public void accept(T t) {
		if (null == t) {
			return;
		}
		try {
			Buffer b;
			if (Buffer.class.isInstance(t)) {
				b = (Buffer) t;
			} else if (null != converter && converter.canConvert(t.getClass(), Buffer.class)) {
				b = converter.convert(t, Buffer.class);
			} else {
				b = Buffer.wrap(t.toString());
			}
			channel.write(b.byteBuffer(), position.getAndAdd(b.remaining()));
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

}
