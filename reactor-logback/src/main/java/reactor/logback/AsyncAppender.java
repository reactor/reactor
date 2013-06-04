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

/**
 * @author Jon Brisbin
 */
public class AsyncAppender extends UnsynchronizedAppenderBase<ILoggingEvent> implements AppenderAttachable<ILoggingEvent> {

	private final AppenderAttachableImpl<ILoggingEvent> attachable      = new AppenderAttachableImpl<ILoggingEvent>();
	private       boolean                               alreadyAttached = false;
	private final Disruptor<LogEvent>  disruptor;
	private       RingBuffer<LogEvent> ringBuffer;

	public AsyncAppender() {
		disruptor = new Disruptor<LogEvent>(
				new EventFactory<LogEvent>() {
					@Override
					public LogEvent newInstance() {
						return new LogEvent();
					}
				},
				512,
				Executors.newFixedThreadPool(1, new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread t = new Thread(r);
						t.setName("logback-ringbuffer-appender");
						t.setPriority(Thread.NORM_PRIORITY);
						t.setDaemon(true);
						return t;
					}
				}),
				ProducerType.MULTI,
				new BlockingWaitStrategy()
		);

		disruptor.handleEventsWith(new LoggingEventHandler());
	}

	@Override
	public void start() {
		ringBuffer = disruptor.start();
		super.start();
	}

	@Override
	public void stop() {
		disruptor.shutdown();
		super.stop();
	}

	@Override
	protected void append(ILoggingEvent loggingEvent) {
		loggingEvent.prepareForDeferredProcessing();
		long seq = ringBuffer.next();
		ringBuffer.get(seq).event = loggingEvent;
		ringBuffer.publish(seq);
	}

	@Override
	public void addAppender(Appender<ILoggingEvent> newAppender) {
		if (!alreadyAttached) {
			alreadyAttached = true;
			attachable.addAppender(newAppender);
		} else {
			addWarn("Cannot attach more than one Appender. Ignoring " + newAppender);
		}
	}

	@Override
	public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
		return attachable.iteratorForAppenders();
	}

	@Override
	public Appender<ILoggingEvent> getAppender(String name) {
		return attachable.getAppender(name);
	}

	@Override
	public boolean isAttached(Appender<ILoggingEvent> appender) {
		return attachable.isAttached(appender);
	}

	@Override
	public void detachAndStopAllAppenders() {
		attachable.detachAndStopAllAppenders();
	}

	@Override
	public boolean detachAppender(Appender<ILoggingEvent> appender) {
		return attachable.detachAppender(appender);
	}

	@Override
	public boolean detachAppender(String name) {
		return attachable.detachAppender(name);
	}

	private static class LogEvent {
		ILoggingEvent event;
	}

	private class LoggingEventHandler implements EventHandler<LogEvent> {
		@Override
		public void onEvent(LogEvent logEvent, long l, boolean b) throws Exception {
			attachable.appendLoopOnAppenders(logEvent.event);
		}
	}

}
