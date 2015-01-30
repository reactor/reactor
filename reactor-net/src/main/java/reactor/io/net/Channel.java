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
import reactor.rx.Promise;

import java.net.InetSocketAddress;

/**
 * {@code NetChannel} is a virtual connection that often matches with a Socket or a Channel (e.g. Netty).
 * Implementations handle interacting inbound (received data) and errors by subscribing to it.
 * Sending data to outbound, effectively replying on that virtual connection, is done via {@link this#send(OUT)} and
 * {@link this#echo(OUT)} for write through.
 *
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public interface Channel<IN, OUT> extends Publisher<IN> {

	/**
	 * Get the address of the remote peer.
	 *
	 * @return the peer's address
	 */
	InetSocketAddress remoteAddress();

	/**
	 * Send data to the peer, listen for any error on write and complete after successful flush or likely behavior.
	 *
	 * @param data
	 * 		the data to send
	 *
	 * @return a {@link reactor.rx.Promise} indicating when the send operation has completed
	 */
	Promise<Void> send(OUT data);

	/**
	 * Send data to the peer, listen for any error on write and complete after write.
	 *
	 * @param data
	 * 		the data to send
	 *
	 * @return a {@link reactor.rx.Promise} indicating when the send operation has completed
	 */
	Promise<Void> echo(OUT data);

	/**
	 * Send data to the peer, listen for any error on write and complete after write.
	 *
	 * @param dataStream
	 * 		the dataStream publishing OUT items to write on this channel
	 *
	 * @return a {@link reactor.rx.Promise} indicating when the send operation has completed
	 */
	Channel<IN, OUT> sink(Publisher<? extends OUT> dataStream);

	/**
	 * Close this {@literal NetChannel} and signal complete to the channel subscribers.
	 */
	void close();

	/**
	 * @return the underlying native connection/channel in use
	 */
	Object nativeConnection();


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
