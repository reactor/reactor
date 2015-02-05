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

package reactor.io.net.http;

import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.PeerStream;
import reactor.io.net.http.model.*;

import java.nio.ByteBuffer;

/**
 * @author Sebastien Deleuze
 * @author Stephane maldini
 */
public abstract class ClientRequest<IN, OUT> extends ChannelStream<IN, OUT> {

	public ClientRequest(Environment env,
	                     Codec<Buffer, IN, OUT> codec,
	                     long prefetch,
	                     PeerStream<IN, OUT,
			                     ChannelStream<IN, OUT>> peer,
	                     Dispatcher ioDispatcher,
	                     Dispatcher eventsDispatcher
	) {
		super(env, codec, prefetch, peer, ioDispatcher, eventsDispatcher);
	}

	/**
	 *
	 * @return
	 */
	public abstract RequestHeaders headers();

	/**
	 *
	 * @return
	 */
	public abstract Protocol protocol();

	/**
	 *
	 * @return
	 */
	public abstract String uri();

	/**
	 *
	 * @return
	 */
	public abstract Method method();

	// RESPONSE contract

	/**
	 *
	 * @return
	 */
	public abstract Status responseStatus();

	/**
	 *
	 * @param status
	 * @return
	 */
	public abstract ClientRequest<IN, OUT> responseStatus(Status status);

	/**
	 *
	 * @return
	 */
	public abstract Transfer transfer();

	/**
	 *
	 * @param transfer
	 * @return
	 */
	public abstract ClientRequest<IN, OUT> transfer(Transfer transfer);

	/**
	 *
	 * @return
	 */
	public abstract ResponseHeaders responseHeaders();

	/**
	 *
	 * @param name
	 * @param value
	 * @return
	 */
	public abstract ClientRequest<IN, OUT> responseHeader(String name, String value);

	@Override
	protected void write(ByteBuffer data, Subscriber<?> onComplete, boolean flush) {

	}

	@Override
	protected void write(Object data, Subscriber<?> onComplete, boolean flush) {

	}

	@Override
	protected void flush() {

	}
}
