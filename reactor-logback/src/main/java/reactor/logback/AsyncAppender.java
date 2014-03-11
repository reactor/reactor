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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import ch.qos.logback.core.spi.ContextAwareBase;
import ch.qos.logback.core.spi.FilterAttachableImpl;
import ch.qos.logback.core.spi.FilterReply;
import reactor.core.processor.Processor;
import reactor.core.processor.spec.ProcessorSpec;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.queue.BlockingQueueFactory;

/**
 * A Logback appender that logs asynchronously, using a {@link reactor.core.processor.Processor} to hold {@link
 * ILoggingEvent logging events} that have yet to be processed.
 *
 * @author Jon Brisbin
 */
public class AsyncAppender
		extends ContextAwareBase
		implements Appender<ILoggingEvent>,
		           AppenderAttachable<ILoggingEvent> {

	private final    AppenderAttachableImpl<ILoggingEvent>    aai          = new AppenderAttachableImpl<ILoggingEvent>();
	private final    FilterAttachableImpl<ILoggingEvent>      fai          = new FilterAttachableImpl<ILoggingEvent>();
	private final    AtomicReference<Appender<ILoggingEvent>> delegate     = new AtomicReference<Appender<ILoggingEvent>>();
	private final    BlockingQueue<ILoggingEvent>             publishQueue = BlockingQueueFactory.createQueue();
	private volatile boolean                                  started      = false;
	private          int                                      backlog      = 1024 * 16;
	private String              name;
	private Processor<LogEvent> processor;
	private Thread              publisher;

	private boolean		    includeCallerData;

	@Override public String getName() {
		return name;
	}

	@Override public void setName(String name) {
		this.name = name;
	}

	/**
	 * Get the backlog size for the internal Reactor {@link reactor.core.processor.Processor}.
	 *
	 * @return the size of the {@link reactor.core.processor.Processor} backlog
	 */
	public int getBacklog() {
		return backlog;
	}

	/**
	 * Set the backlog size for the internal Reactor {@link reactor.core.processor.Processor}.
	 *
	 * @param backlog
	 * 		the size of the {@link reactor.core.processor.Processor} backlog
	 */
	public void setBacklog(int backlog) {
		this.backlog = backlog;
	}

	@Override public void start() {
		processor = new ProcessorSpec<LogEvent>()
				.dataBufferSize(backlog)
				.dataSupplier(new Supplier<LogEvent>() {
					@Override public LogEvent get() {
						return new LogEvent();
					}
				})
				.singleThreadedProducer()
				.when(Throwable.class, new Consumer<Throwable>() {
					@Override public void accept(Throwable t) {
						addError(t.getMessage(), t);
					}
				})
				.consume(new Consumer<LogEvent>() {
					@Override public void accept(LogEvent ev) {
						aai.appendLoopOnAppenders(ev.event);
					}
				})
				.get();

		publisher = new Thread("async-appender-publisher") {
			@Override public void run() {
				final List<ILoggingEvent> batch = new ArrayList<ILoggingEvent>();

				ILoggingEvent ev;
				while(!Thread.currentThread().isInterrupted()) {
					try {
						ev = publishQueue.take();
						batch.add(ev);
						publishQueue.drainTo(batch);

						final ListIterator<ILoggingEvent> events = new ArrayList<ILoggingEvent>(batch).listIterator();
						processor.batch(batch.size(), new Consumer<LogEvent>() {
							public void accept(LogEvent lev) {
								lev.event = events.next();
							}
						});

						batch.clear();
					} catch(InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		};
		publisher.start();

		started = true;
	}

	@Override public void stop() {
		processor.shutdown();
		publisher.interrupt();

		if(null != delegate.get()) {
			delegate.get().stop();
		}

		started = false;
	}

	@Override public void doAppend(ILoggingEvent ev) throws LogbackException {
		if(getFilterChainDecision(ev) == FilterReply.DENY) {
			return;
		}

		ev.prepareForDeferredProcessing();

                if(includeCallerData) {
                    ev.getCallerData();
                }

		publishQueue.add(ev);
	}

	@Override public void addFilter(Filter<ILoggingEvent> newFilter) {
		fai.addFilter(newFilter);
	}

	@Override public void clearAllFilters() {
		fai.clearAllFilters();
	}

	@Override public List<Filter<ILoggingEvent>> getCopyOfAttachedFiltersList() {
		return fai.getCopyOfAttachedFiltersList();
	}

	@Override public FilterReply getFilterChainDecision(ILoggingEvent event) {
		return fai.getFilterChainDecision(event);
	}

	@Override public boolean isStarted() {
		return started;
	}

	@Override public void addAppender(Appender<ILoggingEvent> newAppender) {
		if(delegate.compareAndSet(null, newAppender)) {
			aai.addAppender(newAppender);
		} else {
			throw new IllegalArgumentException(delegate.get() + " already attached.");
		}
	}

	@Override public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
		return aai.iteratorForAppenders();
	}

	@Override public Appender<ILoggingEvent> getAppender(String name) {
		return aai.getAppender(name);
	}

	@Override public boolean isAttached(Appender<ILoggingEvent> appender) {
		return aai.isAttached(appender);
	}

	@Override public void detachAndStopAllAppenders() {
		aai.detachAndStopAllAppenders();
	}

	@Override public boolean detachAppender(Appender<ILoggingEvent> appender) {
		return aai.detachAppender(appender);
	}

	@Override public boolean detachAppender(String name) {
		return aai.detachAppender(name);
	}

        public boolean isIncludeCallerData() {
                return includeCallerData;
        }

        public void setIncludeCallerData(final boolean includeCallerData) {
                this.includeCallerData = includeCallerData;
        }

	private static class LogEvent {
		ILoggingEvent event;
	}

}
