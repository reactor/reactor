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

package reactor.tcp.encoding.syslog;

import java.util.Date;

/**
 * @author Jon Brisbin
 */
public class SyslogMessage {

	private final String raw;
	private final int    priority;
	private final int    facility;
	private final int    severity;
	private final Date   timestamp;
	private final String host;
	private final String message;

	public SyslogMessage(String raw,
											 int priority,
											 int facility,
											 int severity,
											 Date timestamp,
											 String host,
											 String message) {
		this.raw = raw;
		this.priority = priority;
		this.facility = facility;
		this.severity = severity;
		this.timestamp = timestamp;
		this.host = host;
		this.message = message;
	}

	public int getPriority() {
		return priority;
	}

	public int getFacility() {
		return facility;
	}

	public int getSeverity() {
		return severity;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public String getHost() {
		return host;
	}

	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return raw;
	}

}
