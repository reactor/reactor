/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.core.dispatch;

/**
 * <p>Exception thrown when the it is not possible to dispatch an event a {@link reactor.core.Dispatcher}.
 * without it wrapping the consuming sequenes.
 * Used specifically when dispatching with the
 * {@link reactor.core.Dispatcher#tryDispatch(Object, reactor.fn.Consumer, reactor.fn.Consumer)} call.
 *
 * <p>For efficiency this exception will not have a stack trace.
 *
 * original from LMAX Disruptor com.lmax.disruptor.InsufficientCapacityException
 * @author mikeb01
 *
 */
@SuppressWarnings("serial")
public final class InsufficientCapacityException extends RuntimeException
{
	public static final InsufficientCapacityException INSTANCE = new InsufficientCapacityException();

	private InsufficientCapacityException()
	{
		// Singleton
	}

	@Override
	public synchronized Throwable fillInStackTrace()
	{
		return this;
	}
}
