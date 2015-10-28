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

import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveClient;
import reactor.io.net.ReactivePeer;

/**
 * A {@link HttpChannel} callback that is attached on {@link ReactivePeer} or {@link ReactiveClient} initialization
 * and receives
 * all connected {@link HttpChannel}. The {@link #transform implementation must return a Publisher to complete or error
 * in order to close the {@link ReactiveChannel}.
 *
 * @param <IN>  the type of the received data
 * @param <OUT> the type of replied data
 * @author Stephane Maldini
 * @since 2.1
 */
public interface HttpProcessor<IN, OUT, CONN extends HttpChannel<IN, OUT>,
		NEWIN, NEWOUT, NEWCONN extends HttpChannel<NEWIN, NEWOUT>> {

	/**
	 *
	 * @param conn
	 * @return
	 */
	NEWCONN transform(CONN conn);
}
