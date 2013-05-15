package reactor.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import reactor.Fn;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.dispatch.RingBufferDispatcher;

import java.util.Iterator;

/**
 * @author Jon Brisbin
 */
public class AsyncAppender extends UnsynchronizedAppenderBase<ILoggingEvent> implements AppenderAttachable<ILoggingEvent> {

	private static final Dispatcher dispatcher;
	private final AppenderAttachableImpl<ILoggingEvent> attachable      = new AppenderAttachableImpl<ILoggingEvent>();
	private       boolean                               alreadyAttached = false;
	private Reactor reactor;

	static {
		// We only need one thread for all appenders actually
		dispatcher = new RingBufferDispatcher("log", 1, 1024, ProducerType.SINGLE, new BlockingWaitStrategy());
	}

	public AsyncAppender() {
	}

	@Override
	public void start() {
		init();
		super.start();
	}

	@Override
	public void stop() {
		dispatcher.stop();
		super.stop();
	}

	@Override
	protected void append(ILoggingEvent loggingEvent) {
		loggingEvent.prepareForDeferredProcessing();
		reactor.notify(Fn.event(loggingEvent));
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

	private void init() {
		reactor = new Reactor(dispatcher);
		reactor.on(new Consumer<Event<ILoggingEvent>>() {
			@Override
			public void accept(Event<ILoggingEvent> ev) {
				attachable.appendLoopOnAppenders(ev.getData());
			}
		});
	}

}
