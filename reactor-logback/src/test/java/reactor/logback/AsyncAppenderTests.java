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

import ch.qos.logback.classic.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import reactor.support.NamedDaemonThreadFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
		for (int i = 0; i < len; i++) {
			chars[i] = ABCS.charAt(r.nextInt(ABCS.length()));
		}
		MSG = new String(chars);
	}

	final int timeout = 1;
	ExecutorService threadPool;
	Logger          syncLog;
	Logger          asyncLog;
	Logger          chronicleLog;

	@Before
	public void setup() throws IOException {
		Path logDir = Paths.get("log");
		if (Files.exists(logDir)) {
			Files.find(logDir, 1, (pth, attrs) -> pth.toString().endsWith(".log"))
			     .forEach(pth -> {
				     try {
					     Files.delete(pth);
				     } catch (IOException e) {
					     throw new IllegalArgumentException(e.getMessage(), e);
				     }
			     });
		}

		threadPool = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("benchmark-writers"));
		syncLog = (Logger) LoggerFactory.getLogger("sync");
		asyncLog = (Logger) LoggerFactory.getLogger("async");
		chronicleLog = (Logger) LoggerFactory.getLogger("chronicle");
	}

	@After
	public void cleanup() {
		threadPool.shutdownNow();
		chronicleLog.getLoggerContext().stop();
	}

	//@Test
	public void clockSyncAppender() throws InterruptedException {
		long m = benchmarkThread(syncLog, timeout);
		System.out.println("sync: " + (m / timeout) + "/sec");
	}

	//@Test
	public void clockAsyncAppender() throws InterruptedException {
		long n = benchmarkThread(asyncLog, timeout);
		System.out.println("async: " + (n / timeout) + "/sec");
	}

	//@Test
	public void clockChronicleAppender() throws InterruptedException {
		long n = benchmarkThread(chronicleLog, timeout);
		System.out.println("chronicle: " + (n / timeout) + "/sec");
	}

	//@Test
	public void clockAllAppenders() throws InterruptedException {
		clockSyncAppender();
		clockAsyncAppender();
		clockChronicleAppender();
	}

	@Test
	public void dummy() {
	}

	private long benchmarkThread(final Logger logger, int timeout) throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicLong throughput = new AtomicLong(0);

		int threads = Runtime.getRuntime().availableProcessors() * 4;
		for (int i = 0; i < threads; i++) {
			threadPool.submit(new Runnable() {
				@Override
				public void run() {
					while (latch.getCount() > 0) {
						logger.warn("count: {}", throughput.incrementAndGet());
					}
				}
			});
		}
		latch.await(timeout, TimeUnit.SECONDS);
		latch.countDown();

		return throughput.get();
	}

}
