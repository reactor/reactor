package reactor.logback;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import net.openhft.chronicle.*;
import net.openhft.chronicle.tools.ChronicleTools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Jon Brisbin
 */
public class DurableAsyncAppender extends AbstractSingleWriterAppender {

	private final Object writeMonitor = new Object();

	private String basePath = "log";

	private Chronicle       chronicle;
	private ExcerptAppender appender;
	private ExcerptTailer   tailer;

	public DurableAsyncAppender() {
	}

	public String getBasePath() {
		return basePath;
	}

	public void setBasePath(String chronicle) {
		this.basePath = chronicle;
	}

	@Override
	protected void doStart() {
		ChronicleTools.warmup();
		ChronicleConfig config = ChronicleConfig.DEFAULT.clone()
		                                                .synchronousMode(false)
		                                                .useUnsafe(true);
		this.basePath = (this.basePath.endsWith("/") ? this.basePath + getName() : this.basePath + "/" + getName());
		try {
			chronicle = new IndexedChronicle(basePath, config);
			appender = chronicle.createAppender();
			tailer = chronicle.createTailer();

			boolean eventsHandled = false;
			long startIdx = tailer.index();
			List<ILoggingEvent> evts = new ArrayList<ILoggingEvent>();
			while (tailer.nextIndex()) {
				boolean handled = tailer.readBoolean();
				LoggingEvent evt = readLoggingEvent(tailer, (LoggerContext) getContext());
				if (handled) {
					continue;
				}
				evts.add(evt);
				if (tailer.index() % getBacklog() == 0) {
					eventsHandled = true;
					loggingEventsDequeued(evts);
					evts.clear();
				}
			}
			loggingEventsDequeued(evts);
			if (eventsHandled || !evts.isEmpty()) {
				tailer.index((startIdx < 0 ? 0 : startIdx));
				LoggerContext ctx = (LoggerContext) getContext();
				while (tailer.nextIndex()) {
					tailer.writeBoolean(true);
					readLoggingEvent(tailer, ctx);
				}
			}
		} catch (Throwable t) {
			addError(t.getMessage(), t);
		}
	}

	@Override
	protected void doStop() {
		try {
			appender.flush();
			chronicle.close();
			ChronicleTools.deleteOnExit(basePath);
		} catch (IOException e) {
			addError(e.getMessage(), e);
		}
	}

	@Override
	protected void loggingEventQueued(ILoggingEvent evt) {
		writeLoggingEvent(evt);
	}

	@Override
	protected void loggingEventsDequeued(List<ILoggingEvent> evts) {
		int len = evts.size();
		for (int i = 0; i < len; i++) {
			ILoggingEvent evt = evts.get(i);
			getAppenderImpl().appendLoopOnAppenders(evt);
		}
	}

	private void writeLoggingEvent(ILoggingEvent evt) {
		synchronized (writeMonitor) {
			appender.startExcerpt(32 * 1024);
			appender.writeBoolean(false);
			appender.writeLong(evt.getTimeStamp());
			appender.writeObject(evt.getThreadName());
			appender.writeObject(evt.getLoggerName());
			appender.writeInt(evt.getLevel().toInt());
			appender.writeObject(evt.getMessage());
			appender.writeObject(evt.getArgumentArray());
			appender.writeObject(evt.getMDCPropertyMap());

			ThrowableProxy cause = (ThrowableProxy) evt.getThrowableProxy();
			boolean hasCause = null != cause;
			appender.writeBoolean(hasCause);
			if (hasCause) {
				appender.writeObject(cause);
			}
			appender.writeBoolean(isIncludeCallerData());
			if (isIncludeCallerData()) {
				appender.writeObject(evt.getCallerData());
			}

			appender.finish();
		}
	}

	@SuppressWarnings("unchecked")
	private static LoggingEvent readLoggingEvent(ExcerptTailer tailer,
	                                             LoggerContext ctx) {
		long timestamp = tailer.readLong();
		String threadName = tailer.readObject(String.class);
		String loggerName = tailer.readObject(String.class);
		Level level = Level.toLevel(tailer.readInt());
		String msg = tailer.readObject(String.class);
		Object[] args = tailer.readObject(Object[].class);
		Map<String, String> mdcProps = tailer.readObject(Map.class);

		Logger logger = ctx.getLogger(loggerName);
		LoggingEvent evt = new LoggingEvent(
				logger.getClass().getName(),
				logger,
				level,
				msg,
				null,
				args
		);
		evt.setTimeStamp(timestamp);
		evt.setThreadName(threadName);
		evt.setMDCPropertyMap(mdcProps);

		if (tailer.readBoolean()) {
			ThrowableProxy t = tailer.readObject(ThrowableProxy.class);
			evt.setThrowableProxy(t);
		}
		if (tailer.readBoolean()) {
			StackTraceElement[] callerData = tailer.readObject(StackTraceElement[].class);
			evt.setCallerData(callerData);
		}

		return evt;
	}

}
