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

package reactor.logback;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import ch.qos.logback.classic.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * @author Jon Brisbin
 */
public class AsyncAppenderTests {

	static final String MSG;

	static {
		String ABCS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
		Random r = new Random();

		char[] chars = new char[20000];
		int len = chars.length;
		for(int i = 0; i < len; i++) {
			chars[i] = ABCS.charAt(r.nextInt(ABCS.length()));
		}
		MSG = new String(chars);
	}

	final int timeout = 5;
	ExecutorService threadPool;

	@Before
	public void setup() {
		threadPool = Executors.newCachedThreadPool();
	}

	@After
	public void cleanup() {
		threadPool.shutdownNow();
	}

	@Test
	public void clockAsyncAppender() throws InterruptedException {
		long n = benchmarkThread((Logger)LoggerFactory.getLogger("reactor"), timeout);
		System.out.println("async: " + (n / timeout) + "/sec");
	}

	@Test
	public void clockSyncAppender() throws InterruptedException {
		long m = benchmarkThread((Logger)LoggerFactory.getLogger("sync"), timeout);
		System.out.println("sync: " + (m / timeout) + "/sec");
	}

	@Ignore
	@Test
	public void clockBothAppenders() throws InterruptedException {
		clockSyncAppender();
		clockAsyncAppender();
	}

	private long benchmarkThread(final Logger logger, int timeout) throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicLong throughput = new AtomicLong(0);

		int threads = Runtime.getRuntime().availableProcessors() * 4;
		for(int i = 0; i < threads; i++) {
			threadPool.submit(new Runnable() {
				@Override
				public void run() {
					while(latch.getCount() > 0) {
						logger.warn("" + throughput.incrementAndGet());
					}
				}
			});
		}

		latch.await(timeout, TimeUnit.SECONDS);
		latch.countDown();
		return throughput.get();
	}

}
