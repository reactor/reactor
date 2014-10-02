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

package reactor.net.encoding.syslog;

import java.util.Date;

/**
 * An object representation of a syslog message
 *
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

	/**
	 * Creates a new syslog message.
	 *
	 * @param raw The raw, unparsed message
	 * @param priority The message's priority
	 * @param facility The message's facility
	 * @param severity The message's severity
	 * @param timestamp The message's timestamp
	 * @param host The host from which the message originated
	 * @param message The actual message
	 */
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

	/**
	 * Returns the priority assigned to the message
	 *
	 * @return The message's priority
	 */
	public int getPriority() {
		return priority;
	}

	/**
	 * Returns the facility that sent the message
	 *
	 * @return The message's facility
	 */
	public int getFacility() {
		return facility;
	}

	/**
	 * Returns the severity assigned to the message
	 *
	 * @return The message's severity
	 */
	public int getSeverity() {
		return severity;
	}

	/**
	 * Returns the timestamp for the message
	 *
	 * @return The message's timestamp
	 */
	public Date getTimestamp() {
		return timestamp;
	}

	/**
	 * Returns the host from which the message originated
	 *
	 * @return The message's host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Returns the actual message
	 *
	 * @return The text-based message
	 */
	public String getMessage() {
		return message;
	}

	@Override
	public String toString() {
		return raw;
	}

}
