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

package reactor.queue;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * A {@literal ConcurrentLinkedQueue} that supports a terminal state.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
final public class CompletableConcurrentLinkedQueue<T> extends ConcurrentLinkedQueue<T> implements CompletableQueue<T>{

	boolean terminated = false;

	@Override
	public void complete() {
		terminated = true;
	}

	@Override
	public boolean isComplete() {
		return terminated;
	}
}
