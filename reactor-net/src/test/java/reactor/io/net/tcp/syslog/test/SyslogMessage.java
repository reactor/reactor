/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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

package reactor.io.net.tcp.syslog.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SyslogMessage {


	private static final Pattern TAG_AND_CONTENT_PATTERN = Pattern.compile("([A-Za-z0-9]+)(.*)");

	private final int facility;

	private final Severity severity;

	private final Timestamp timestamp;

	private final String host;

	private final String message;

	SyslogMessage(int facility, Severity severity, Timestamp timestamp, String host, String message) {
		this.facility = facility;
		this.severity = severity;
		this.timestamp = timestamp;
		this.host = host;
		this.message = message;
	}

	public int getFacility() {
		return facility;
	}

	public Severity getSeverity() {
		return severity;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public String getHost() {
		return host;
	}

	public String getMessage() {
		return message;
	}

	public String getTag() {
		Matcher matcher = TAG_AND_CONTENT_PATTERN.matcher(message);
		if (matcher.matches()) {
			return matcher.group(1);
		} else {
			return null;
		}
	}

	public String getContent() {
		Matcher matcher = TAG_AND_CONTENT_PATTERN.matcher(message);
		if (matcher.matches()) {
			return matcher.group(2);
		} else {
			return null;
		}
	}

	@Override
	public String toString() {
		return "SyslogMessage [facility=" + facility + ", severity=" + severity + ", timestamp=" + timestamp + ", host=" + host + ", message=" + message + "]";
	}
}