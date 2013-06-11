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

package reactor.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import com.lmax.disruptor.*;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jon Brisbin
 */
public class AsyncAppender extends UnsynchronizedAppenderBase<ILoggingEvent> implements AppenderAttachable<ILoggingEvent> {

	private final AppenderAttachableImpl<ILoggingEvent>    aai      = new AppenderAttachableImpl<ILoggingEvent>();
	private final AtomicReference<Appender<ILoggingEvent>> delegate = new AtomicReference<Appender<ILoggingEvent>>();
	private ExecutorService      threadPool;
	private RingBuffer<LogEvent> ringBuffer;
	private WorkerPool<LogEvent> workerPool;

	public AsyncAppender() {
	}

	@Override
	public void start() {
		this.threadPool = Executors.newCachedThreadPool(
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread t = new Thread(r);
						t.setName("logback-ringbuffer-appender");
						t.setPriority(Thread.MAX_PRIORITY);
						t.setDaemon(true);
						return t;
					}
				}
		);

		this.workerPool = new WorkerPool<LogEvent>(
				new EventFactory<LogEvent>() {
					@Override
					public LogEvent newInstance() {
						return new LogEvent();
					}
				},
				new ExceptionHandler() {
					@Override
					public void handleEventException(Throwable throwable, long l, Object o) {
						addError(throwable.getMessage(), throwable);
					}

					@Override
					public void handleOnStartException(Throwable throwable) {
						addError(throwable.getMessage(), throwable);
					}

					@Override
					public void handleOnShutdownException(Throwable throwable) {
						addError(throwable.getMessage(), throwable);
					}
				},
				new LogEventHandler()
		);
		this.ringBuffer = workerPool.start(threadPool);

		super.start();
	}

	@Override
	public void stop() {
		workerPool.halt();
		threadPool.shutdown();

		if (null != delegate.get()) {
			delegate.get().stop();
		}

		super.stop();
	}

	@Override
	protected void append(ILoggingEvent loggingEvent) {
		if (null != delegate.get()) {
			long seq = ringBuffer.next();
			LogEvent evt = ringBuffer.get(seq);
			evt.event = loggingEvent;
			ringBuffer.publish(seq);
		}
	}

	@Override
	public void addAppender(Appender<ILoggingEvent> newAppender) {
		if (delegate.compareAndSet(null, newAppender)) {
			aai.addAppender(newAppender);
		} else {
			throw new IllegalArgumentException(delegate.get() + " already attached.");
		}
	}

	@Override
	public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
		return aai.iteratorForAppenders();
	}

	@Override
	public Appender<ILoggingEvent> getAppender(String name) {
		return aai.getAppender(name);
	}

	@Override
	public boolean isAttached(Appender<ILoggingEvent> appender) {
		return aai.isAttached(appender);
	}

	@Override
	public void detachAndStopAllAppenders() {
		aai.detachAndStopAllAppenders();
	}

	@Override
	public boolean detachAppender(Appender<ILoggingEvent> appender) {
		return aai.detachAppender(appender);
	}

	@Override
	public boolean detachAppender(String name) {
		return aai.detachAppender(name);
	}

	private static class LogEvent {
		ILoggingEvent event;
	}

	private class LogEventHandler implements WorkHandler<LogEvent> {
		@Override
		public void onEvent(LogEvent logEvent) throws Exception {
			aai.appendLoopOnAppenders(logEvent.event);
			logEvent.event = null;
		}
	}

}
