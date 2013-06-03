package reactor.tcp.syslog;

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
