/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Portions of this code:
 * Copyright (c) 2003-2009 Johann Burkard (jb@eaio.com) http://eaio.com.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package reactor.util;

import com.eaio.uuid.UUIDGen;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Jon Brisbin
 */
public abstract class UUIDUtils {

	private static final long          clockNodeAndSeq = UUIDGen.getClockSeqAndNode();
	private static final ReentrantLock lock            = new ReentrantLock();
	private static final AtomicLong    lastTime        = new AtomicLong();

	private UUIDUtils() {
	}

	public static UUID create() {
		long timeMillis = (System.currentTimeMillis() * 10000) + 0x01B21DD213814000L;

		lock.lock();
		try {
			if (lastTime.get() == timeMillis) {
				timeMillis = lastTime.incrementAndGet();
			} else {
				lastTime.set(timeMillis);
			}
		} finally {
			lock.unlock();
		}

		// time low
		long time = timeMillis << 32;

		// time mid
		time |= (timeMillis & 0xFFFF00000000L) >> 16;

		// time hi and version
		time |= 0x1000 | ((timeMillis >> 48) & 0x0FFF); // version 1

		return new UUID(time, clockNodeAndSeq);
	}

}
