/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.io;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.reactivestreams.PublisherFactory;
import reactor.core.reactivestreams.SubscriberWithContext;
import reactor.core.support.ReactorFatalException;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;

/**
 * A factory for Reactive basic IO operations such as File read/write, Byte read and Codec decoding.
 *
 * @author Stephane Maldini
 * @since 2.0.4
 */
public final class IO {

	private IO() {
	}


	/**
	 * Use the given {@link Codec} to decode any {@code SRC} data published by the {@code publisher} reference.
	 * Some codec might result into N signals for one SRC data.
	 *
	 * @param codec     the codec decoder is going to be used to scan the incoming {@code SRC} data
	 * @param publisher The data stream publisher we want to decode
	 * @return a Publisher of decoded values
	 */
	public static Publisher<Buffer> read(final ReadableByteChannel channel) {
		return read(channel, -1);
	}

	/**
	 * Use the given {@link Codec} to decode any {@code SRC} data published by the {@code publisher} reference.
	 * Some codec might result into N signals for one SRC data.
	 *
	 * @param codec     the codec decoder is going to be used to scan the incoming {@code SRC} data
	 * @param publisher The data stream publisher we want to decode
	 * @return a Publisher of decoded values
	 */
	public static Publisher<Buffer> read(final ReadableByteChannel channel, int chunkSize) {
		return PublisherFactory.forEach(
				chunkSize < 0 ? defaultChannelReadConsumer : new ChannelReadConsumer(chunkSize),
				new Function<Subscriber<? super Buffer>, ReadableByteChannel>() {
					@Override
					public ReadableByteChannel apply(Subscriber<? super Buffer> subscriber) {
						return channel;
					}
				},
				channelCloseConsumer
		);
	}

	/**
	 * Use the given {@link Codec} to decode any {@code SRC} data published by the {@code publisher} reference.
	 * Some codec might result into N signals for one SRC data.
	 *
	 * @param codec     the codec decoder is going to be used to scan the incoming {@code SRC} data
	 * @param publisher The data stream publisher we want to decode
	 * @return a Publisher of decoded values
	 */
	public static Publisher<Buffer> readFile(Path path) {
		return readFile(path.getParent().toString(), path.getFileName().toString(), -1);
	}


	/**
	 * Use the given {@link Codec} to decode any {@code SRC} data published by the {@code publisher} reference.
	 * Some codec might result into N signals for one SRC data.
	 *
	 * @param codec     the codec decoder is going to be used to scan the incoming {@code SRC} data
	 * @param publisher The data stream publisher we want to decode
	 * @return a Publisher of decoded values
	 */
	public static Publisher<Buffer> readFile(Path path, int chunkSize) {
		return readFile(path.getParent().toString(), path.getFileName().toString(), chunkSize);
	}

	/**
	 * Use the given {@link Codec} to decode any {@code SRC} data published by the {@code publisher} reference.
	 * Some codec might result into N signals for one SRC data.
	 *
	 * @return a Publisher of decoded values
	 */
	public static Publisher<Buffer> readFile(final String path, final String filename) {
		return readFile(path, filename, -1);
	}

	/**
	 * Use the given {@link Codec} to decode any {@code SRC} data published by the {@code publisher} reference.
	 * Some codec might result into N signals for one SRC data.
	 *
	 * @return a Publisher of decoded values
	 */
	public static Publisher<Buffer> readFile(final String path, final String filename, int chunkSize) {
		return PublisherFactory.forEach(
				chunkSize < 0 ? defaultChannelReadConsumer : new ChannelReadConsumer(chunkSize),
				new Function<Subscriber<? super Buffer>, ReadableByteChannel>() {
					@Override
					public ReadableByteChannel apply(Subscriber<? super Buffer> subscriber) {
						try{
							RandomAccessFile file = new RandomAccessFile(path, filename);
							return new FileContext(file);
						}catch (FileNotFoundException e){
							throw ReactorFatalException.create(e);
						}
					}
				},
				channelCloseConsumer);
	}

	private static final ChannelCloseConsumer channelCloseConsumer       = new ChannelCloseConsumer();
	private static final ChannelReadConsumer  defaultChannelReadConsumer = new ChannelReadConsumer(Buffer
			.SMALL_BUFFER_SIZE);

	/**
	 * A read access to the source file
	 */
	public static final class FileContext implements ReadableByteChannel{
		private final RandomAccessFile file;
		private final ReadableByteChannel channel;

		public FileContext(RandomAccessFile file) {
			this.file = file;
			this.channel = file.getChannel();
		}

		public RandomAccessFile file(){
			return file;
		}

		@Override
		public int read(ByteBuffer dst) throws IOException {
			return channel.read(dst);
		}

		@Override
		public boolean isOpen() {
			return channel.isOpen();
		}

		@Override
		public void close() throws IOException {
			channel.close();
		}
	}

	private static final class ChannelReadConsumer implements Consumer<SubscriberWithContext<Buffer,
			ReadableByteChannel>> {

		private final int bufferSize;

		public ChannelReadConsumer(int bufferSize) {
			this.bufferSize = bufferSize;
		}

		@Override
		public void accept(SubscriberWithContext<Buffer, ReadableByteChannel> sub) {
			try {
				ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
				if (sub.context().read(buffer) > 0) {
					buffer.flip();
					sub.onNext(new Buffer(buffer));
				} else {
					sub.onComplete();
				}
			} catch (IOException e) {
				sub.onError(e);
			}
		}
	}

	private static final class ChannelCloseConsumer implements Consumer<ReadableByteChannel> {
		@Override
		public void accept(ReadableByteChannel channel) {
			try {
				if (channel != null) {
					channel.close();
				}
			} catch (IOException ioe) {
				throw new IllegalStateException(ioe);
			}
		}
	}


}
