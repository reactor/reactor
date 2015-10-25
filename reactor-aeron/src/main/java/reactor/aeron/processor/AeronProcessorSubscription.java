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
package reactor.aeron.processor;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.BackpressureUtils;
import reactor.io.buffer.Buffer;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.MutableDirectBuffer;

/**
 * @author Anatoly Kadyshev
 */
class AeronProcessorSubscription implements Subscription {

	private final Subscriber<? super Buffer> subscriber;

	private final AeronHelper aeronHelper;

	private final Publication commandPub;

	private volatile boolean active = true;

	private static final int DEFAULT_FRAGMENT_LIMIT = 64;

	private final RequestCounter requestCounter;

	AeronProcessorSubscription(Subscriber<? super Buffer> subscriber, int fragmentLimit, AeronHelper aeronHelper,
							   Publication commandPub) {
		this.subscriber = subscriber;
		this.aeronHelper = aeronHelper;
		this.commandPub = commandPub;
		this.requestCounter = new RequestCounter(fragmentLimit > 0 ? fragmentLimit : DEFAULT_FRAGMENT_LIMIT);
	}

	@Override
	public void request(long n) {
		if (BackpressureUtils.checkRequest(n, subscriber)) {
			sendRequestCommand(n);
			requestCounter.request(n);
		}
	}

	@Override
	public void cancel() {
		active = false;
	}

	void sendRequestCommand(long n) {
		BufferClaim bufferClaim = aeronHelper.publish(commandPub, new BufferClaim(), 9,
		  AeronHelper.newBackoffIdleStrategy());
		if (bufferClaim != null) {
			try {
				MutableDirectBuffer mutableBuffer = bufferClaim.buffer();
				int offset = bufferClaim.offset();
				mutableBuffer.putByte(offset, CommandType.Request.getCode());
				mutableBuffer.putLong(offset + 1, n);
			} finally {
				bufferClaim.commit();
			}
		}
	}

	public boolean isActive() {
		return active;
	}

	public RequestCounter getRequestCounter() {
		return requestCounter;
	}
}
