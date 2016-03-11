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

import java.util.Calendar;

class SyslogMessageParser {

	private static final int MAXIMUM_SEVERITY = 7;

	private static final int MAXIMUM_FACILITY = 23;

	private static final int MINIMUM_PRI = 0;

	private static final int MAXIMUM_PRI = (MAXIMUM_FACILITY * 8) + MAXIMUM_SEVERITY;

	private static final int DEFAULT_PRI = 13;

	public static SyslogMessage parse(String raw) {
		if (null == raw || raw.isEmpty()) {
			return null;
		}

		int pri = DEFAULT_PRI;
		Timestamp timestamp = null;
		String host = null;

		int index = 0;

		if (raw.indexOf('<') == 0) {
			int endPri = raw.indexOf('>');
			if (endPri >= 2 && endPri <= 4) {
				int candidatePri = Integer.parseInt(raw.substring(1, endPri));
				if (candidatePri >= MINIMUM_PRI && candidatePri <= MAXIMUM_PRI) {
					pri = candidatePri;
					index = endPri + 1;
					timestamp = new Timestamp(raw.substring(index, index + 15));
					index += 16;
					int endHostnameIndex = raw.indexOf(' ', index);
					host = raw.substring(index, endHostnameIndex);
					index = endHostnameIndex + 1;
				}
			}
		}

		if (host == null) {
			// TODO Need a way to populate host with client's IP address
		}

		int facility = pri / 8;
		Severity severity = Severity.values()[pri % 8];

		if (timestamp == null) {
			timestamp = new Timestamp(Calendar.getInstance());
		}

		return new SyslogMessage(facility, severity, timestamp, host, raw.substring(index));
	}

}
