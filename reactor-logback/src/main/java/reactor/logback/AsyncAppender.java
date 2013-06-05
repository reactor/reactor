package reactor.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Jon Brisbin
 */
public class AsyncAppender extends UnsynchronizedAppenderBase<ILoggingEvent> implements AppenderAttachable<ILoggingEvent> {

	private final AppenderAttachableImpl<ILoggingEvent>    aai      = new AppenderAttachableImpl<ILoggingEvent>();
	private final AtomicReference<Appender<ILoggingEvent>> delegate = new AtomicReference<Appender<ILoggingEvent>>();
	private       int                                      backlog  = 8 * 1024;
	private Disruptor<LogEvent>  disruptor;
	private RingBuffer<LogEvent> ringBuffer;

	public AsyncAppender() {
	}

	public void setBacklog(int backlog) {
		this.backlog = backlog;
	}

	@Override
	public void start() {
		disruptor = new Disruptor<LogEvent>(
				new EventFactory<LogEvent>() {
					@Override
					public LogEvent newInstance() {
						return new LogEvent();
					}
				},
				backlog,
				Executors.newFixedThreadPool(
						1,
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
				),
				ProducerType.MULTI,
				new BlockingWaitStrategy()
		);
		disruptor.handleEventsWith(new LoggingEventHandler());
		ringBuffer = disruptor.start();

		super.start();
	}

	@Override
	public void stop() {
		disruptor.shutdown();
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

	private class LoggingEventHandler implements EventHandler<LogEvent> {
		@Override
		public void onEvent(LogEvent logEvent, long l, boolean b) throws Exception {
			aai.appendLoopOnAppenders(logEvent.event);
			logEvent.event = null;
		}
	}

}
