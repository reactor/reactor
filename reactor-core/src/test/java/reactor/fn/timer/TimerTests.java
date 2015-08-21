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
package reactor.fn.timer;

import org.junit.Assert;
import org.junit.Test;
import reactor.fn.Pausable;

import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author @masterav10
 */
public class TimerTests {

	@Test
	public void verifyPause() throws InterruptedException {
		HashWheelTimer timer = new HashWheelTimer();

		AtomicInteger count = new AtomicInteger();

		int tasks = 10;
		Phaser phaser = new Phaser(tasks);

		AtomicLong sysTime = new AtomicLong();

		Pausable pausable = timer.schedule((time) -> {
			if (phaser.getPhase() == 0) {
				phaser.arrive();
				sysTime.set(System.nanoTime());
			}
			count.getAndIncrement();
		}, 100, TimeUnit.MILLISECONDS, 500);

		phaser.awaitAdvance(0);

		pausable.pause();
		long time = System.nanoTime() - sysTime.get();
		Thread.sleep(1000);
		HashWheelTimer.TimerPausable<?> registration = (HashWheelTimer.TimerPausable<?>) pausable;
		Assert.assertTrue(registration.isPaused());
		Assert.assertTrue(time < TimeUnit.MILLISECONDS.toNanos(100));
		Assert.assertEquals(tasks, count.get());
		timer.cancel();
	}
}
