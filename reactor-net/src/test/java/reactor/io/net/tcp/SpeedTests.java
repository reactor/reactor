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

package reactor.io.netty.tcp;

import org.junit.Ignore;
import org.junit.Test;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jon Brisbin
 */
public class SpeedTests {

	final static int runs       = 100000000;
	final static int iterations = 3;

	@Test
	@Ignore
	public void clockIntegerParseInt() {
		Consumer<Integer> test = new Consumer<Integer>() {
			byte[] bytes = "34".getBytes();

			@Override
			public void accept(Integer integer) {
				String s = new String(bytes);
				Integer.parseInt(s);
			}
		};

		doTest("parseInt", test);
	}

	@Test
	@Ignore
	public void clockByteBufferGetInt() {
		final Buffer b = Buffer.wrap("34");

		Consumer<Integer> test = new Consumer<Integer>() {
			@Override
			public void accept(Integer integer) {
				Buffer.parseInt(b);
			}
		};

		doTest("char shifting", test);
	}

	@Test
	@Ignore
	public void clockSimpleDateFormatParse() {
		final SimpleDateFormat rfc3414 = new SimpleDateFormat("MMM d HH:mm:ss");
		final byte[] bytes = "Oct 11 22:14:15".getBytes();

		Consumer<Integer> test = new Consumer<Integer>() {
			@Override
			public void accept(Integer integer) {
				try {
					rfc3414.parse(new String(bytes));
				} catch (ParseException e) {
					throw new IllegalStateException(e);
				}
			}
		};

		doTest("RFC3414 parse", test);
	}

	@Test
	@Ignore
	public void clockManualDateExtraction() {
		final String[] months = new String[]{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
		final Buffer date = Buffer.wrap("Oct 11 22:14:15");
		final Iterable<Buffer.View> slices = date.slice(0, 3, 4, 6, 7, 9, 10, 12, 13);

		Consumer<Integer> test = new Consumer<Integer>() {
			Calendar cal = Calendar.getInstance();

			@Override
			public void accept(Integer integer) {
				Iterator<Buffer.View> iter = slices.iterator();

				Buffer b = iter.next().get();
				int month = Arrays.binarySearch(months, b.asString());
				int day = Buffer.parseInt(iter.next().get());
				int hr = Buffer.parseInt(iter.next().get());
				int min = Buffer.parseInt(iter.next().get());
				int sec = Buffer.parseInt(iter.next().get());

				cal.set(2013, month, day, hr, min, sec);
				cal.getTime();
			}
		};

		doTest("slice parse", test);
	}

	@Test
	@Ignore
	public void clockRegexExtraction() {
		final String[] months = new String[]{"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
		final Pattern pattern = Pattern.compile("([A-Z][a-z]{2})[\\s]+(\\d{1,2})[\\s](\\d{2}):(\\d{2}):(\\d{2})");
		final Buffer date = Buffer.wrap("Oct 11 22:14:15");

		Consumer<Integer> test = new Consumer<Integer>() {
			Calendar cal = Calendar.getInstance();

			@Override
			public void accept(Integer integer) {
				Matcher m = pattern.matcher(date.asString());
				m.matches();

				int month = Arrays.binarySearch(months, m.group(1));
				int day = Integer.valueOf(m.group(2));
				int hr = Integer.valueOf(m.group(2));
				int min = Integer.valueOf(m.group(2));
				int sec = Integer.valueOf(m.group(2));

				cal.set(2013, month, day, hr, min, sec);
				cal.getTime();
			}
		};

		doTest("regex parse", test);
	}

	private void doTest(String name, Consumer<Integer> test) {
		for (int j = 0; j < iterations; j++) {
			int runs = SpeedTests.runs / 100;
			long start = System.currentTimeMillis();
			for (int i = 0; i < runs; i++) {
				test.accept(i);
			}
			long end = System.currentTimeMillis();
			long elapsed = end - start;
			int throughput = (int) ((runs * 1.0) / ((elapsed * 1.0) / 1000));

			System.out.println(name + " (run #" + (j + 1) + "): \t" + throughput + "/sec");
		}
	}

}
