package reactor.core.action;

import reactor.core.HashWheelTimer;
import reactor.core.Observable;
import reactor.event.Event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class SlidingWindowAction<T> extends WindowAction<T> {

  private final ReentrantLock lock            = new ReentrantLock();
  private final T[]           collectedWindow;
  private final AtomicInteger pointer = new AtomicInteger(0);

  public SlidingWindowAction(Observable d,
                             Object successKey,
                             Object failureKey,
                             HashWheelTimer timer,
                             int period,
                             TimeUnit timeUnit,
                             int delay,
                             int backlog) {
    super(d, successKey, failureKey, timer, period, timeUnit, delay);
    this.collectedWindow = (T[]) new Object[backlog];
  }

  @Override
  protected void doWindow(Long aLong) {
    lock.lock();
    try {
      int currentPointer = pointer.get();

      List<T> window = new ArrayList<T>();
      if (currentPointer > collectedWindow.length) {
        for(int i = currentPointer; i < collectedWindow.length; i++) {
          window.add(collectedWindow[i]);
        }
      }

      for(int i = 0; i < currentPointer; i++) {
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
      int index = pointer.getAndIncrement() & (collectedWindow.length - 1);
      collectedWindow[index] = value.getData();
    } finally {
      lock.unlock();
    }
  }
}
