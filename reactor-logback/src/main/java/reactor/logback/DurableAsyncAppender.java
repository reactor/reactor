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
import ch.qos.logback.classic.spi.LoggingEvent;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.tools.ChronicleTools;

import java.io.IOException;

/**
 * An {@literal AsyncAppender} subclass that first writes a log event to a durable {@literal Chronicle} using Java
 * Chronicle before allowing the event to be queued.
 *
 * @author Jon Brisbin
 */
public class DurableAsyncAppender extends AsyncAppender {

	private final Object writeMonitor = new Object();

	private String basePath = "log";

	private Chronicle       chronicle;
	private ExcerptAppender appender;

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
		this.basePath = (this.basePath.endsWith("/") ? this.basePath + getName() : this.basePath + "/" + getName());
		try {
			chronicle = ChronicleQueueBuilder.indexed(basePath).synchronous(true).build();
			appender = chronicle.createAppender();
		} catch (Throwable t) {
			addError(t.getMessage(), t);
		}
	}

	@Override
	protected void doStop() {
		try {
			appender.flush();
			chronicle.close();
		} catch (IOException e) {
			addError(e.getMessage(), e);
		}
	}

	@Override
	protected void queueLoggingEvent(ILoggingEvent evt) {
		synchronized (writeMonitor) {
			LoggingEventRecord.write(appender, (LoggingEvent) evt, isIncludeCallerData(), 1);
		}
		super.queueLoggingEvent(evt);
	}

}
