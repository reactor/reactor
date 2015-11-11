/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.aeron.processor;

import com.beust.jcommander.internal.Lists;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Anatoly Kadyshev
 */
class ThreadSnapshot {

	private List<String> beforeThreadNames;

	public ThreadSnapshot take() {
		if (beforeThreadNames != null) {
			throw new IllegalStateException("Thread snapshot was already taken");
		}
		beforeThreadNames = takeThreadNamesSnapshot();
		return this;
	}

	public boolean takeAndCompare(long timeoutMillis) throws InterruptedException {
		return takeAndCompare(new String[] { }, timeoutMillis);
	}

	public boolean takeAndCompare(String[] excludedPrefixes, long timeoutMillis) throws InterruptedException {
		List<String> liveThreadNames;
		long startTime = System.nanoTime();
		while ( (liveThreadNames = getLiveThreadNames(excludedPrefixes)).size() > 0){
			Thread.sleep(100);

			if (System.nanoTime() - startTime > TimeUnit.MILLISECONDS.toNanos(timeoutMillis)) {
				System.err.println("Ouch! These threads were not terminated: " + liveThreadNames);
				return false;
			}
		}
		return true;
	}

	private List<String> getLiveThreadNames(String[] excludedPrefixes) {
		List<String> afterThreadNames = takeThreadNamesSnapshot();
		afterThreadNames.removeAll(beforeThreadNames);
		return afterThreadNames.stream().filter(new Predicate<String>() {
			@Override
			public boolean test(String s) {
				for (String prefix : excludedPrefixes) {
					if (s.startsWith(prefix)) {
						return false;
					}
				}
				return true;
			}
		}).collect(Collectors.toList());
	}

	private List<String> takeThreadNamesSnapshot() {
		Thread[] tarray;
		int activeCount;
		int actualCount;
		do {
			activeCount = Thread.activeCount();
			tarray = new Thread[activeCount];
			actualCount = Thread.enumerate(tarray);
		} while (activeCount != actualCount);

		List<String> threadNames = Lists.newArrayList();
		for (int i = 0; i < actualCount; i++) {
			threadNames.add(tarray[i].getName());
		}

		return threadNames;
	}

}
