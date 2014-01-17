package reactor.core;

import reactor.pool.Pool;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public abstract class ObjectPool<T extends Releasable> implements Pool<T> {

  private List<T> pool;
  private BitSet leaseMask;
  private ReentrantLock lock;

  public ObjectPool(int prealloc) {
    this.pool = new ArrayList<T>();
    this.preallocate(prealloc);
    this.leaseMask = new BitSet(prealloc);
    this.lock = new ReentrantLock();
  }

  @Override
  public T allocate() {
    lock.lock();
    try {
      int nextClear = leaseMask.nextClearBit(0);

      if (nextClear == pool.size()) {
        pool.add(newInstance(nextClear));
      }
      leaseMask.set(nextClear);
      return pool.get(nextClear);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void deallocate(T obj) {
    // obj.free();
  }

  public void deallocate(int poolPosition) {
    lock.lock();
    try {
      leaseMask.clear(poolPosition);
    } finally {
      lock.unlock();
    }

  }

  private void preallocate(int amount) {
    for(int i = 0; i < amount; i++) {
      this.pool.add(newInstance(i));
    }
  }

  public abstract T newInstance(int poolPosition);
}