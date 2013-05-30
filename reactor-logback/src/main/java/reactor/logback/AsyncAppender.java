package reactor.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import reactor.core.Environment;
import reactor.core.R;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;

import java.util.Iterator;

/**
 * @author Jon Brisbin
 */
public class AsyncAppender extends UnsynchronizedAppenderBase<ILoggingEvent> implements AppenderAttachable<ILoggingEvent> {

	private final Environment                           env             = new Environment() {
		@Override
		protected String getDefaultProfile() {
			return "logback";
		}
	};
	private final AppenderAttachableImpl<ILoggingEvent> attachable      = new AppenderAttachableImpl<ILoggingEvent>();
	private       boolean                               alreadyAttached = false;
	private Reactor reactor;

	public AsyncAppender() {
	}

	@Override
	public void start() {
		init();
		super.start();
	}

	@Override
	public void stop() {
		super.stop();
	}

	@Override
	protected void append(ILoggingEvent loggingEvent) {
		loggingEvent.prepareForDeferredProcessing();
		reactor.notify(Event.wrap(loggingEvent));
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
		reactor = R.reactor().using(env).ringBuffer().get();
		reactor.on(new Consumer<Event<ILoggingEvent>>() {
			@Override
			public void accept(Event<ILoggingEvent> ev) {
				attachable.appendLoopOnAppenders(ev.getData());
			}
		});
	}

}
