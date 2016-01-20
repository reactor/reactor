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

package reactor.alloc;

import reactor.core.timer.Timers;
import reactor.core.util.ReactiveState;

/**
 * An abstract {@link Reference} implementation that does reference counting.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class AbstractReference<T extends ReactiveState.Recyclable> implements Reference<T> {

	private volatile int refCnt = 0;

	private final long inception;
	private final T    obj;

	protected AbstractReference(T obj) {
		this.obj = obj;
		this.inception = Timers.currentTimeMillisResolver().get();
	}

	@Override
	public long getAge() {
		return Timers.currentTimeMillisResolver().get() - inception;
	}

	@Override
	public int getReferenceCount() {
		return refCnt;
	}

	@Override
	public void retain() {
		retain(1);
	}

	@Override
	public void retain(int incr) {
		refCnt += incr;
	}

	@Override
	public void release() {
		release(1);
	}

	@Override
	public void release(int decr) {
		refCnt -= Math.min(decr, refCnt);
		if (refCnt < 1) {
			obj.recycle();
		}
	}

	@Override
	public T get() {
		return obj;
	}

	@Override
	public String toString() {
		return "Reference{" +
		  "refCnt=" + refCnt +
		  ", inception=" + inception +
		  ", obj=" + obj +
		  '}';
	}

}
