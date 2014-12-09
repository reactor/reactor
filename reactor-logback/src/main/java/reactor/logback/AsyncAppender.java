/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.*;
import reactor.core.processor.Operation;
import reactor.core.processor.Processor;
import reactor.core.processor.spec.ProcessorSpec;
import reactor.function.Consumer;
import reactor.function.Supplier;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Logback {@literal Appender} implementation that uses a Reactor {@link reactor.core.processor.Processor} internally
 * to queue events to a single-writer thread. This implementation doesn't do any actually appending itself, it just
 * delegates to a "real" appender but it uses the efficient queueing mechanism of the {@literal RingBuffer} to do so.
 *
 * @author Jon Brisbin
 */
public class AsyncAppender
		extends ContextAwareBase
		implements Appender<ILoggingEvent>,
		           AppenderAttachable<ILoggingEvent> {

	private final AppenderAttachableImpl<ILoggingEvent>    aai      = new AppenderAttachableImpl<ILoggingEvent>();
	private final FilterAttachableImpl<ILoggingEvent>      fai      = new FilterAttachableImpl<ILoggingEvent>();
	private final AtomicReference<Appender<ILoggingEvent>> delegate = new AtomicReference<Appender<ILoggingEvent>>();

	private String              name;
	private Processor<LogEvent> processor;

	private long    backlog           = 1024 * 1024;
	private boolean includeCallerData = false;
	private boolean started           = false;

	public long getBacklog() {
		return backlog;
	}

	public void setBacklog(long backlog) {
		this.backlog = backlog;
	}

	public boolean isIncludeCallerData() {
		return includeCallerData;
	}

	public void setIncludeCallerData(final boolean includeCallerData) {
		this.includeCallerData = includeCallerData;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public boolean isStarted() {
		return started;
	}

	@Override
	public void doAppend(ILoggingEvent evt) throws LogbackException {
		if (getFilterChainDecision(evt) == FilterReply.DENY) {
			return;
		}
		evt.prepareForDeferredProcessing();
		if (includeCallerData) {
			evt.getCallerData();
		}
		try {
			queueLoggingEvent(evt);
		} catch (Throwable t) {
			addError(t.getMessage(), t);
		}
	}

	@Override
	public void start() {
    startDelegateAppender();

		processor = new ProcessorSpec<LogEvent>()
				.dataSupplier(new Supplier<LogEvent>() {
					@Override
					public LogEvent get() {
						return new LogEvent();
					}
				})
				.multiThreadedProducer()
				.dataBufferSize((int) backlog)
				.when(Throwable.class, new Consumer<Throwable>() {
					@Override
					public void accept(Throwable throwable) {
						addError(throwable.getMessage(), throwable);
					}
				})
				.consume(new Consumer<LogEvent>() {
					@Override
					public void accept(LogEvent evt) {
						loggingEventDequeued(evt.event);
					}
				})
				.get();

		try {
			doStart();
		} catch (Throwable t) {
			addError(t.getMessage(), t);
		} finally {
			started = true;
		}
	}

  private void startDelegateAppender() {
    Appender<ILoggingEvent> delegateAppender = delegate.get();
    if (null != delegateAppender && !delegateAppender.isStarted()) {
      delegateAppender.start();
    }
  }

	@Override
	public void stop() {
		if (null != delegate.get()) {
			delegate.get().stop();
		}
		aai.detachAndStopAllAppenders();

		processor.shutdown();

		try {
			doStop();
		} catch (Throwable t) {
			addError(t.getMessage(), t);
		} finally {
			started = false;
		}
	}

	@Override
	public void addFilter(Filter<ILoggingEvent> newFilter) {
		fai.addFilter(newFilter);
	}

	@Override
	public void clearAllFilters() {
		fai.clearAllFilters();
	}

	@Override
	public List<Filter<ILoggingEvent>> getCopyOfAttachedFiltersList() {
		return fai.getCopyOfAttachedFiltersList();
	}

	@Override
	public FilterReply getFilterChainDecision(ILoggingEvent event) {
		return fai.getFilterChainDecision(event);
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

	protected AppenderAttachableImpl<ILoggingEvent> getAppenderImpl() {
		return aai;
	}

	protected void doStart() {
	}

	protected void doStop() {
	}

	protected void queueLoggingEvent(ILoggingEvent evt) {
		if (null != delegate.get()) {
			Operation<LogEvent> op = processor.prepare();
			op.get().event = evt;
			op.commit();
		}
	}

	protected void loggingEventDequeued(ILoggingEvent evt) {
		aai.appendLoopOnAppenders(evt);
	}

	private static class LogEvent {
		ILoggingEvent event;
	}

}
