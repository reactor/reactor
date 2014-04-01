package reactor.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.*;
import reactor.support.NamedDaemonThreadFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jon Brisbin
 */
public abstract class AbstractSingleWriterAppender
		extends ContextAwareBase
		implements Appender<ILoggingEvent>,
		           AppenderAttachable<ILoggingEvent> {

	private final AppenderAttachableImpl<ILoggingEvent>    aai      = new AppenderAttachableImpl<ILoggingEvent>();
	private final FilterAttachableImpl<ILoggingEvent>      fai      = new FilterAttachableImpl<ILoggingEvent>();
	private final AtomicReference<Appender<ILoggingEvent>> delegate = new AtomicReference<Appender<ILoggingEvent>>();

	private String                       name;
	private BlockingQueue<ILoggingEvent> eventQueue;
	private ExecutorService              executor;
	private Future<?>                    workerFuture;

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
		eventQueue.add(evt);
		loggingEventQueued(evt);
	}

	@Override
	public void start() {
		if (null != delegate.get()) {
			delegate.get().start();
		}
		eventQueue = new ArrayBlockingQueue<ILoggingEvent>((int) backlog, true);

		executor = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory(getName()));
		workerFuture = executor.submit(new Runnable() {
			@Override
			public void run() {
				ILoggingEvent evt;
				for (; ; ) {
					try {
						evt = eventQueue.take();
						List<ILoggingEvent> evts = new ArrayList<ILoggingEvent>();
						evts.add(evt);
						eventQueue.drainTo(evts);
						loggingEventsDequeued(evts);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						break;
					}
				}
			}
		});

		doStart();

		started = true;
	}

	@Override
	public void stop() {
		if (null != delegate.get()) {
			delegate.get().stop();
		}
		aai.detachAndStopAllAppenders();
		workerFuture.cancel(true);
		executor.shutdown();

		doStop();

		started = false;
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

	protected abstract void doStart();

	protected abstract void doStop();

	protected abstract void loggingEventQueued(ILoggingEvent evt);

	protected abstract void loggingEventsDequeued(List<ILoggingEvent> evts);

}
