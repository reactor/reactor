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

import java.net.InetSocketAddress;

/**
 * {@code Channel} is a virtual connection that often matches with a Socket or a Channel (e.g. Netty).
 * Implementations handle interacting inbound (received data) and errors by subscribing to it.
 *
 * Writing and "flushing" is controlled by sinking 1 or more {@link #writeWith(Publisher)}
 * that will forward data to outbound.
 * When a drained Publisher completes or error, the channel will automatically "flush" its pending writes.
 *
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public interface ReactorChannel<IN, OUT> extends Publisher<IN> {

	/**
	 * Get the address of the remote peer.
	 *
	 * @return the peer's address
	 */
	InetSocketAddress remoteAddress();

	/**
	 * Send data to the peer, listen for any error on write and close on terminal signal (complete|error).
	 * If more than one publisher is attached (multiple calls to writeWith()) completion occurs after all publishers complete.
	 *
	 * @param dataStream
	 * 		the dataStream publishing OUT items to write on this channel
	 *
	 * @return A Publisher to signal successful sequence write (e.g. after "flush") or any error during write
	 */
	Publisher<Void> writeWith(Publisher<? extends OUT> dataStream);


	/**
	 * Assign event handlers to certain channel lifecycle events.
	 *
	 * @return ConsumerSpec to build the events handlers
	 */
	ConsumerSpec on();

	/**
	 * Spec class for assigning multiple event handlers on a channel.
	 */
	interface ConsumerSpec {
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
