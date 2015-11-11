/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.io.net;

import reactor.rx.Promise;
import reactor.rx.Promises;

/**
 * Base functionality needed by all reactor peers
 *
 * @param <IN>  The type that will be received by this server
 * @param <OUT> The type that will be sent by this server
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public abstract class ReactorPeer<IN, OUT, PEER extends ReactivePeer<IN, OUT, ? extends ReactiveChannel<IN, OUT>>> {

	protected final PEER peer;

	protected ReactorPeer(PEER peer) {
		this.peer = peer;
	}

	/**
	 * Shutdown this {@literal ReactorPeer} and complete the returned {@link Promise<Void>}
	 * when shut down.
	 * @return a {@link Promise<Void>} that will be complete when the {@link
	 * ReactivePeer} is shutdown
	 */
	public Promise<Void> shutdown() {
		return Promises.from(peer.shutdown());
	}


	/**
	 *
	 * @return
	 */
	public PEER delegate() {
		return peer;
	}
}
