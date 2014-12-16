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
package reactor.core.queue.internal;

/**
 * @author Stephane Maldini
 */
import reactor.core.internal.PlatformDependent;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

abstract class MpscLinkedQueueTailRef<E> extends MpscLinkedQueuePad1<E> {

	private static final long serialVersionUID = 8717072462993327429L;

	@SuppressWarnings("rawtypes")
	private static final AtomicReferenceFieldUpdater<MpscLinkedQueueTailRef, MpscLinkedQueueNode> UPDATER;

	static {
		@SuppressWarnings("rawtypes")
		AtomicReferenceFieldUpdater<MpscLinkedQueueTailRef, MpscLinkedQueueNode> updater;
		updater = PlatformDependent.newAtomicReferenceFieldUpdater(MpscLinkedQueueTailRef.class, "tailRef");
		if (updater == null) {
			updater = AtomicReferenceFieldUpdater.newUpdater(
					MpscLinkedQueueTailRef.class, MpscLinkedQueueNode.class, "tailRef");
		}
		UPDATER = updater;
	}

	private transient volatile MpscLinkedQueueNode<E> tailRef;

	protected final MpscLinkedQueueNode<E> tailRef() {
		return tailRef;
	}

	protected final void setTailRef(MpscLinkedQueueNode<E> tailRef) {
		this.tailRef = tailRef;
	}

	@SuppressWarnings("unchecked")
	protected final MpscLinkedQueueNode<E> getAndSetTailRef(MpscLinkedQueueNode<E> tailRef) {
		// LOCK XCHG in JDK8, a CAS loop in JDK 7/6
		return (MpscLinkedQueueNode<E>) UPDATER.getAndSet(this, tailRef);
	}
}
