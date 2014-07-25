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

package reactor.alloc;

import com.gs.collections.impl.list.mutable.FastList;
import org.junit.Ignore;
import org.junit.Test;
import reactor.AbstractPerformanceTest;
import reactor.core.Environment;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * @author Jon Brisbin
 */
public class AllocationTests extends AbstractPerformanceTest {

	@Override
	public void setup() {
		super.setup();
		pool = Executors.newFixedThreadPool(Environment.PROCESSORS);
	}

	@Test
	@Ignore
	public void threadPartitionedAllocatorAllocatesByThread() throws Exception {
		int threadCnt = Environment.PROCESSORS;
		CountDownLatch latch = new CountDownLatch(threadCnt);
		Allocator<RecyclableNumber> alloc = new ThreadPartitionedAllocator<>(RecyclableNumber::new);
		List<Long> threadIds = FastList.newList();

		fork(threadCnt, () -> {
			long threadId = Thread.currentThread().getId();
			alloc.allocate().get().setValue(threadId);
			threadIds.add(threadId);
		});

		fork(threadCnt, () -> {
			if (threadIds.contains(alloc.allocate().get().longValue())) {
				latch.countDown();
			}
		});

		assertTrue("latch was counted down", latch.await(5, TimeUnit.SECONDS));
	}

}
