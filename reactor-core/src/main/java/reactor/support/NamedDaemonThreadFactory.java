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

package reactor.support;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jon Brisbin
 */
public class NamedDaemonThreadFactory implements ThreadFactory {

	private static final AtomicInteger COUNTER = new AtomicInteger(0);
	private final String prefix;

	public NamedDaemonThreadFactory(String prefix) {
		this.prefix = prefix;
	}

	@Override public Thread newThread(Runnable runnable) {
		Thread t = new Thread(runnable);
		t.setName(prefix + "-" + COUNTER.incrementAndGet());
		t.setDaemon(true);
		return t;
	}

}
