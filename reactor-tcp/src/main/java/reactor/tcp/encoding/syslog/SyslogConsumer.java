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

package reactor.tcp.encoding.syslog;

import reactor.fn.Consumer;
import reactor.tcp.TcpConnection;

/**
 * Abstract helper class to aid in consuming {@link SyslogMessage SyslogMessages}.
 *
 * @author Jon Brisbin
 */
public abstract class SyslogConsumer implements Consumer<TcpConnection<SyslogMessage, Void>> {

	@Override
	public final void accept(TcpConnection<SyslogMessage, Void> conn) {
		conn.consume(new Consumer<SyslogMessage>() {
			@Override
			public void accept(SyslogMessage syslogMessage) {
				SyslogConsumer.this.accept(syslogMessage);
			}
		});
	}

	/**
	 * Implementations of this class should override this method to act on messages received.
	 *
	 * @param msg The incoming syslog message.
	 */
	protected abstract void accept(SyslogMessage msg);

}
