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
package reactor.core.action;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.timer.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MovingWindowAction is collecting maximum {@param backlog} events on a stream
 * until {@param period} is reached, after that steams collected events further,
 * and continues collecting without clearing the list.
 */
public class MovingWindowAction<T> extends WindowAction<T> {

  private final ReentrantLock lock    = new ReentrantLock();
  private final AtomicInteger pointer = new AtomicInteger(0);
  private final T[]           collectedWindow;

	@SuppressWarnings("unchecked")
  public MovingWindowAction(Observable d,
                            Object successKey,
                            Object failureKey,
                            Timer timer,
                            int period, TimeUnit timeUnit, int delay, int backlog
  ) {
    super(d, successKey, failureKey, timer, period, timeUnit, delay);
    this.collectedWindow = (T[]) new Object[backlog];
  }

  @Override
  protected void doWindow(Long aLong) {
    lock.lock();
    try {
      int currentPointer = pointer.get();

      List<T> window = new ArrayList<T>();
      int adjustedPointer = adjustPointer(currentPointer);

      if (currentPointer > collectedWindow.length) {
        for(int i = adjustedPointer; i < collectedWindow.length; i++) {
          window.add(collectedWindow[i]);
        }
      }

      for(int i = 0; i < adjustedPointer; i++) {
        window.add(collectedWindow[i]);
      }

      notifyValue(Event.wrap(window));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void doAccept(Event<T> value) {
    lock.lock();
    try {
      int index = adjustPointer(pointer.getAndIncrement());
      collectedWindow[index] = value.getData();
    } finally {
      lock.unlock();
    }
  }

  private int adjustPointer(int pointer) {
    return pointer % collectedWindow.length;
  }
}