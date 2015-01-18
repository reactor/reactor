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

package reactor.io.net;

import org.reactivestreams.Publisher;
import reactor.fn.Consumer;
import reactor.fn.batch.BatchConsumer;
import reactor.rx.Promise;
import reactor.rx.Stream;

import java.net.InetSocketAddress;

/**
 * {@code NetChannel} is a virtual connection that often matches with a Socket or a Channel (e.g. Netty).
 * Implementations handle interacting inbound (received data) and errors by subscribing to it.
 * Sending data to outbound, effectively replying on that virtual connection, is done via {@link this#send(OUT)} and
 * {@link this#echo(OUT)} or {@link this#echoFrom(Publisher)} for fire and forget.
 *
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public interface NetChannel<IN, OUT> extends Publisher<IN> {

	/**
	 * Get the address of the remote peer.
	 *
	 * @return the peer's address
	 */
	InetSocketAddress remoteAddress();

	/**
	 * {@link reactor.rx.Stream} of incoming decoded data.
	 *
	 * @return input {@link reactor.rx.Stream}
	 */
	Stream<IN> in();

	/**
	 * {@link reactor.fn.batch.BatchConsumer} for efficiently data to the peer.
	 *
	 * @return output {@link reactor.fn.batch.BatchConsumer}
	 */
	BatchConsumer<OUT> out();

	/**
	 * Send data to the peer, listen for any error on write and complete after successful write.
	 *
	 * @param data
	 * 		the data to send
	 *
	 * @return a {@link reactor.rx.Promise} indicating when the send operation has completed
	 */
	Promise<Void> send(OUT data);

	/**
	 * Send data to the peer.
	 *
	 * @param data
	 * 		the data to send
	 *
	 * @return {@literal this}
	 */
	NetChannel<IN, OUT> echo(OUT data);

	/**
	 * Send data to the peer that passes through the given {@link reactor.rx.Stream}.
	 *
	 * @param data
	 * 		the {@link reactor.rx.Stream} of data to monitor
	 *
	 * @return {@literal this}
	 */
	NetChannel<IN, OUT> echoFrom(Publisher<? extends OUT> data);

	/**
	 * Close this {@literal NetChannel} and signal complete to the contentStream (@link this#in()).
	 */
	void close();

	/**
	 * Assign event handlers to certain channel lifecycle events.
	 *
	 * @return ConsumerSpec to build the events handlers
	 */
	ConsumerSpec on();

	/**
	 * Spec class for assigning multiple event handlers on a channel.
	 */
	public static interface ConsumerSpec {
		/**
		 * Assign a {@link Runnable} to be invoked when the channel is closed.
		 *
		 * @param onClose
		 * 		the close event handler
		 *
		 * @return {@literal this}
		 */
		ConsumerSpec close(Consumer<Void> onClose);

		/**
		 * Assign a {@link Runnable} to be invoked when reads have become idle for the given timeout.
		 *
		 * @param idleTimeout
		 * 		the idle timeout
		 * @param onReadIdle
		 * 		the idle timeout handler
		 *
		 * @return {@literal this}
		 */
		ConsumerSpec readIdle(long idleTimeout, Consumer<Void> onReadIdle);

		/**
		 * Assign a {@link Runnable} to be invoked when writes have become idle for the given timeout.
		 *
		 * @param idleTimeout
		 * 		the idle timeout
		 * @param onWriteIdle
		 * 		the idle timeout handler
		 *
		 * @return {@literal this}
		 */
		ConsumerSpec writeIdle(long idleTimeout, Consumer<Void> onWriteIdle);
	}

}
