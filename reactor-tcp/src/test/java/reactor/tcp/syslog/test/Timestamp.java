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

package reactor.tcp.syslog.test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

public final class Timestamp {

	private static final List<String> MONTHS = Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec");
	private final int month;
	private final int day;
	private final int hours;
	private final int minutes;
	private final int seconds;

	Timestamp(Calendar instance) {
		month = instance.get(Calendar.MONTH);
		day = instance.get(Calendar.DAY_OF_MONTH);
		hours = instance.get(Calendar.HOUR_OF_DAY);
		minutes = instance.get(Calendar.MINUTE);
		seconds = instance.get(Calendar.SECOND);
	}

	Timestamp(String toParse) {
		String monthString = toParse.substring(0, 3);
		month = MONTHS.indexOf(monthString);
		day = Integer.parseInt(toParse.substring(4, 6));
		hours = Integer.parseInt(toParse.substring(7, 9));
		minutes = Integer.parseInt(toParse.substring(10, 12));
		seconds = Integer.parseInt(toParse.substring(13));
	}

	public int getMonth() {
		return month;
	}

	public int getDay() {
		return day;
	}

	public int getHours() {
		return hours;
	}

	public int getMinutes() {
		return minutes;
	}

	public int getSeconds() {
		return seconds;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder(MONTHS.get(month));
		builder.append(" ");
		builder.append(day);
		builder.append(" ");
		builder.append(hours);
		builder.append(":");
		builder.append(minutes);
		builder.append(":");
		builder.append(seconds);
		return builder.toString();
	}
}
