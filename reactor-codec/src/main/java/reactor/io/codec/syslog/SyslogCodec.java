/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

package reactor.io.codec.syslog;

import java.util.Calendar;
import java.util.Date;

import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;

/**
 * A coded for consuming syslog messages. This codec produces no output, i.e.  its encoding
 * function returns {@code null}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class SyslogCodec extends Codec<Buffer, SyslogMessage, Void> {

	private static final int MAXIMUM_SEVERITY = 7;
	private static final int MAXIMUM_FACILITY = 23;
	private static final int MINIMUM_PRI      = 0;
	private static final int MAXIMUM_PRI      = (MAXIMUM_FACILITY * 8) + MAXIMUM_SEVERITY;
	private static final int DEFAULT_PRI      = 13;

	@Override
	public Buffer apply(Void v) {
		return null;
	}

	@Override
	public Function<Buffer, SyslogMessage> decoder(Consumer<SyslogMessage> next) {
		return new SyslogMessageDecoder(next);
	}

	@Override
	protected SyslogMessage decodeNext(Buffer buffer, Object context) {
		return null;
	}

	private class SyslogMessageDecoder implements Function<Buffer, SyslogMessage> {
		private final Calendar cal  = Calendar.getInstance();
		private final int      year = cal.get(Calendar.YEAR);
		private final Consumer<SyslogMessage> next;
		private       Buffer.View             remainder;

		private SyslogMessageDecoder(Consumer<SyslogMessage> next) {
			this.next = next;
		}

		@Override
		public SyslogMessage apply(Buffer buffer) {
			return parse(buffer);
		}

		private SyslogMessage parse(Buffer buffer) {
			String line = null;
			if (null != remainder) {
				line = remainder.get().asString();
			}

			int start = 0;
			for (Buffer.View view : buffer.split('\n', false)) {
				Buffer b = view.get();
				if (b.last() != '\n') {
					remainder = view;
					return null;
				}
				String s = b.asString();
				if (null != line) {
					line += s;
				} else {
					line = s;
				}
				if (line.isEmpty()) {
					continue;
				}

				int priority = DEFAULT_PRI;
				int facility = priority / 8;
				int severity = priority % 8;

				int priStart = line.indexOf('<', start);
				int priEnd = line.indexOf('>', start + 1);
				if (priStart == 0) {
					int pri = Buffer.parseInt(b, 1, priEnd);
					if (pri >= MINIMUM_PRI && pri <= MAXIMUM_PRI) {
						priority = pri;
						facility = priority / 8;
						severity = priority % 8;
					}
					start = 4;
				}

				Date tstamp = parseRfc3414Date(b, start, start + 15);
				String host = null;
				if (null != tstamp) {
					start += 16;
					int end = line.indexOf(' ', start);
					host = line.substring(start, end);
					if (null != host) {
						start += host.length() + 1;
					}
				}

				String msg = line.substring(start);

				SyslogMessage syslogMsg = new SyslogMessage(line,
						priority,
						facility,
						severity,
						tstamp,
						host,
						msg);
				if (null != next) {
					next.accept(syslogMsg);
				} else {
					return syslogMsg;
				}

				line = null;
				start = 0;
			}

			return null;
		}

		private Date parseRfc3414Date(Buffer b, int start, int end) {
			b.snapshot();

			b.byteBuffer().limit(end);
			b.byteBuffer().position(start);

			int month = -1;
			int day = -1;
			int hr = -1;
			int min = -1;
			int sec = -1;

			switch (b.read()) {
				case 'A': // Apr, Aug
					switch (b.read()) {
						case 'p':
							month = Calendar.APRIL;
							b.read();
							break;
						default:
							month = Calendar.AUGUST;
							b.read();
					}
					break;
				case 'D': // Dec
					month = Calendar.DECEMBER;
					b.read();
					b.read();
					break;
				case 'F': // Feb
					month = Calendar.FEBRUARY;
					b.read();
					b.read();
					break;
				case 'J': // Jan, Jun, Jul
					switch (b.read()) {
						case 'a':
							month = Calendar.JANUARY;
							b.read();
							break;
						default:
							switch (b.read()) {
								case 'n':
									month = Calendar.JUNE;
									break;
								default:
									month = Calendar.JULY;
							}
					}
					break;
				case 'M': // Mar, May
					b.read();
					switch (b.read()) {
						case 'r':
							month = Calendar.MARCH;
							break;
						default:
							month = Calendar.MAY;
					}
					break;
				case 'N': // Nov
					month = Calendar.NOVEMBER;
					b.read();
					b.read();
					break;
				case 'O': // Oct
					month = Calendar.OCTOBER;
					b.read();
					b.read();
					break;
				case 'S': // Sep
					month = Calendar.SEPTEMBER;
					b.read();
					b.read();
					break;
				default:
					return null;
			}

			while (b.read() == ' ') {
			}

			int dayStart = b.position() - 1;
			while (b.read() != ' ') {
			}
			int dayEnd = b.position() - 1;
			day = Buffer.parseInt(b, dayStart, dayEnd);

			while (b.read() == ' ') {
			}

			int timeStart = b.position() - 1;
			hr = Buffer.parseInt(b, timeStart, timeStart + 2);
			min = Buffer.parseInt(b, timeStart + 3, timeStart + 5);
			sec = Buffer.parseInt(b, timeStart + 6, timeStart + 8);

			try {
				if (month < 0 || day < 0 || hr < 0 || min < 0 || sec < 0) {
					return null;
				} else {
					cal.set(year, month, day, hr, min, sec);
					return cal.getTime();
				}
			} finally {
				b.reset();
			}
		}
	}

}
