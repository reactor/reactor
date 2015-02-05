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

package reactor.io.net.http.model;

/**
 *
 * @author Sebastien Deleuze
 */
public enum Transfer {

	/**
	 * Load in memory all the stream data before sending it as a non chunked HTTP response.
	 */
	NON_CHUNKED("non-chunked"),

	/**
	 * Send the response stream as HTTP chunks.
	 * @see <a href="https://tools.ietf.org/html/rfc7230#section-4.1">Chunked transfer encoding</a>
	 */
	CHUNKED("chunked"),

	/**
	 * Send the response stream as Server-Sent Events.
	 * @see <a href="http://dev.w3.org/html5/eventsource/">Server-Sent Events specification</a>
 	 */
	EVENT_STREAM("event-stream");

	private final String transfer;

	Transfer(String transfer) {
		this.transfer = transfer;
	}

	public static Transfer get(String transfer) {
		if (transfer.equals(NON_CHUNKED.transfer)) return NON_CHUNKED;
		if (transfer.equals(CHUNKED.transfer)) return CHUNKED;
		if (transfer.equals(EVENT_STREAM.transfer)) return EVENT_STREAM;
		throw new IllegalArgumentException("Unexpected transfer: " + transfer);
	}

	@Override public String toString() {
		return this.transfer;
	}

}
