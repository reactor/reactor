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

import ch.qos.logback.classic.spi.LoggingEvent;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jon Brisbin
 */
class LoggingEventRecord implements Serializable {

	private static final long   serialVersionUID = 4286033251454846145L;
	private static final String CRLF             = "\r\n";

	private long                timestamp;
	private String              threadName;
	private String              loggerName;
	private int                 level;
	private String              message;
	private Object[]            args;
	private StackTraceElement[] callerData;
	private Map<String, String> mdcProps;
	private Throwable           cause;

	public LoggingEventRecord() {
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getThreadName() {
		return threadName;
	}

	public String getLoggerName() {
		return loggerName;
	}

	public int getLevel() {
		return level;
	}

	public String getMessage() {
		return message;
	}

	public Object[] getArgs() {
		return args;
	}

	public StackTraceElement[] getCallerData() {
		return callerData;
	}

	public Map<String, String> getMdcProps() {
		return mdcProps;
	}

	public Throwable getCause() {
		return cause;
	}

	static void write(ExcerptAppender ex, LoggingEvent evt, boolean includeCallerData, int vers) {
		ex.startExcerpt(32 * 1024);
		ex.writeInt(vers);

		if (vers == 1) {
			ex.writeLong(evt.getTimeStamp());
			ex.writeInt(evt.getLevel().toInt());

			Object[] args = evt.getArgumentArray();
			int argLen = (null != args ? args.length : 0);
			ex.writeInt(argLen);

			Map<String, String> mdcProps = evt.getMDCPropertyMap();
			int propsLen = (null != mdcProps ? mdcProps.size() : 0);
			ex.writeInt(propsLen);

			StackTraceElement[] callerData = null;
			if (includeCallerData) {
				callerData = evt.getCallerData();
			}
			int callerDataLen = (null != callerData ? callerData.length : 0);
			ex.writeInt(callerDataLen);

			ex.writeUTF(evt.getThreadName());
			ex.writeUTF(evt.getLoggerName());
			ex.writeUTF(evt.getMessage());

			for (int i = 0; i < argLen; i++) {
				ex.writeUTF(args[i].toString());
			}

			for (Map.Entry<String, String> entry : evt.getMDCPropertyMap().entrySet()) {
				ex.writeUTF(entry.getKey());
				ex.writeUTF(entry.getValue());
			}

			for (int i = 0; i < callerDataLen; i++) {
				ex.writeObject(callerData[i]);
			}

			boolean hasCause = null != evt.getThrowableProxy();
			ex.writeBoolean(hasCause);
			if (hasCause) {
				ex.writeObject((Throwable) evt.getThrowableProxy());
			}
		}

		ex.finish();
	}

	@SuppressWarnings("unchecked")
	static LoggingEventRecord read(ExcerptTailer ex) {
		int vers = ex.readInt();
		if (vers == 1) {
			LoggingEventRecord rec = new LoggingEventRecord();

			rec.timestamp = ex.readLong();
			rec.level = ex.readInt();
			int argLen = ex.readInt();
			int propsLen = ex.readInt();
			int callerDataLen = ex.readInt();

			rec.threadName = ex.readUTF();
			rec.loggerName = ex.readUTF();
			rec.message = ex.readUTF();

			String[] args = new String[argLen];
			for (int i = 0; i < argLen; i++) {
				args[i] = ex.readUTF();
			}
			rec.args = args;

			Map<String, String> mdcProps = (propsLen > 0
			  ? new HashMap<String, String>()
			  : Collections.<String, String>emptyMap());
			for (int i = 0; i < propsLen; i++) {
				String key = ex.readUTF();
				String val = ex.readUTF();
				mdcProps.put(key, val);
			}
			rec.mdcProps = mdcProps;

			StackTraceElement[] callerData = new StackTraceElement[callerDataLen];
			for (int i = 0; i < callerDataLen; i++) {
				callerData[i] = ex.readObject(StackTraceElement.class);
			}
			rec.callerData = callerData;

			if (ex.readBoolean()) {
				rec.cause = ex.readObject(Throwable.class);
			}

			return rec;
		}
		throw new IllegalStateException("Version " + vers + " not supported");
	}

}
