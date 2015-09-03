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
package reactor.core.processor.simple;

import org.reactivestreams.Subscriber;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.support.SignalType;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Utility methods to perform common tasks associated with {@link Subscriber} handling when the
 * signals are stored in a {@link java.util.Queue}.
 */
public final class SimpleSubscriberUtils {

	private SimpleSubscriberUtils() {
	}

	public static <E> void onNext(E value, Queue<SimpleSignal<E>> queue) {
		while (!queue.offer(SimpleSignal.onNext(value))) {
			LockSupport.parkNanos(1);
		}
	}

	public static <E> void onError(Throwable error, Queue<SimpleSignal<E>> queue) {
		while (!queue.offer(SimpleSignal.<E>onError(error))) {
			LockSupport.parkNanos(1);
		}
	}

	public static <E> void onComplete(Queue<SimpleSignal<E>> queue) {
		while (!queue.offer(SimpleSignal.<E>onComplete())) {
			LockSupport.parkNanos(1);
		}
	}

	public static <E> void route(SimpleSignal<E> task, Subscriber<? super E> subscriber) {
		if (task.type == SignalType.NEXT && null != task.value) {
			// most likely case first
			subscriber.onNext(task.value);
		} else if (task.type == SignalType.COMPLETE) {
			// second most likely case next
			subscriber.onComplete();
			throw CancelException.INSTANCE;
		} else if (task.type == SignalType.ERROR) {
			// errors should be relatively infrequent compared to other signals
			subscriber.onError(task.error);
			throw CancelException.INSTANCE;
		}

	}

	public static <T> boolean waitRequestOrTerminalEvent(
	  AtomicLong pendingRequest,
	  Queue<SimpleSignal<T>> queue,
	  Subscriber<? super T> subscriber
	) {
		SimpleSignal<T> event = null;
		while (pendingRequest.get() <= 0l) {
			//pause until first request
			if (event == null) {
				event = queue.peek();

				if (event == null) {
					if (pendingRequest.get() == Long.MIN_VALUE / 2) {
						return false;
					}
					continue;
				}

				if (event.type == SignalType.COMPLETE) {
					try {
						subscriber.onComplete();
						return false;
					} catch (Throwable t) {
						Exceptions.throwIfFatal(t);
						subscriber.onError(t);
						return false;
					}
				} else if (event.type == SignalType.ERROR) {
					subscriber.onError(event.error);
					return false;
				}
			} else if (pendingRequest.get() == Long.MIN_VALUE / 2) {
				return false;
			}
			LockSupport.parkNanos(1l);
		}

		return true;
	}

}
