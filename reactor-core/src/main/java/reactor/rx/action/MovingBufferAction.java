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
package reactor.rx.action;

import reactor.event.dispatch.Dispatcher;

import java.util.ArrayList;
import java.util.List;

/**
 * MovingWindowAction is collecting maximum {@param backlog} events on a stream
 * after that steams collected events further,
 * and continues collecting without clearing the list.
 *
 * @since 2.0
 */
public class MovingBufferAction<T> extends BatchAction<T, List<T>> {

	private final T[] collectedWindow;

	private int pointer = 0;

	@SuppressWarnings("unchecked")
	public MovingBufferAction(Dispatcher dispatcher, int backlog
	) {
		super(backlog, dispatcher, true, false, true);
		this.collectedWindow = (T[]) new Object[backlog];
	}

	@Override
	protected void flushCallback(T value) {
		List<T> currentBuffer = new ArrayList<T>();
		int adjustedPointer = adjustPointer(pointer);

		if (pointer > collectedWindow.length) {
			for (int i = adjustedPointer; i < collectedWindow.length; i++) {
				currentBuffer.add(collectedWindow[i]);
			}
		}

		for (int i = 0; i < adjustedPointer; i++) {
			currentBuffer.add(collectedWindow[i]);
		}

		if (!currentBuffer.isEmpty()) {
			broadcastNext(currentBuffer);
		}
	}

	@Override
	protected void nextCallback(T value) {
		int index = adjustPointer(pointer);
		if(++pointer < 0) pointer = 0;
		collectedWindow[index] = value;
	}

	private int adjustPointer(int pointer) {
		return pointer % collectedWindow.length;
	}

}