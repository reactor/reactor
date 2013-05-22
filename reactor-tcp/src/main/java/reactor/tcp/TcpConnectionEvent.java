/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.tcp;

import reactor.fn.Event;

/**
 * ApplicationEvent representing certain operations on a {@link TcpConnection}.
 * @author Gary Russell
 *
 */
public class TcpConnectionEvent extends Event<TcpConnection<?>> {

	/**
	 * Valid EventTypes - subclasses should provide similar enums for
	 * their types.
	 *
	 */
	public enum TcpConnectionEventType implements EventType {
		OPEN,
		CLOSE,
		EXCEPTION;
	}

	private final EventType type;

	private final String connectionFactoryName;

	private final Throwable throwable;

	public TcpConnectionEvent(TcpConnection<?> connection, EventType type,
			String connectionFactoryName) {
		super(connection);
		this.type = type;
		this.throwable = null;
		this.connectionFactoryName = connectionFactoryName;
	}

	public TcpConnectionEvent(TcpConnection<?> connection, Throwable t,
			String connectionFactoryName) {
		super(connection);
		this.type = TcpConnectionEventType.EXCEPTION;
		this.throwable = t;
		this.connectionFactoryName = connectionFactoryName;
	}

	public EventType getType() {
		return this.type;
	}

	public String getConnectionId() {
		return this.getData().getConnectionId();
	}

	public String getConnectionFactoryName() {
		return this.connectionFactoryName;
	}

	public Throwable getThrowable() {
		return this.throwable;
	}

	@Override
	public String toString() {
		return "TcpConnectionEvent [type=" + this.getType() +
				", factory=" + this.connectionFactoryName +
				", connectionId=" + this.getConnectionId() +
			   (this.throwable == null ? "" : ", Exception=" + this.throwable) + "]";
	}

	/**
	 * A marker interface allowing the definition of enums for allowed event types.
	 *
	 */
	public interface EventType {

	}

}
