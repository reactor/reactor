package reactor.pipe.registry;

import reactor.bus.registry.Registration;
import reactor.bus.selector.Selector;

public final class DelayedRegistration<K, V> implements Registration<K, V> {

    private final Selector<K> selector;
    private final V           object;

    public DelayedRegistration(Selector<K> selector, V object) {
        this.selector = selector;
        this.object = object;
    }

    @Override
    public Selector<K> getSelector() {
        return selector;
    }

    @Override
    public V getObject() {
        return object;
    }

    @Override
    public Registration<K, V> cancelAfterUse() {
        return null;
    }

    @Override
    public boolean isCancelAfterUse() {
        return false;
    }

    @Override
    public void cancel() {

    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public void pause() {

    }

    @Override
    public boolean isPaused() {
        return false;
    }

    @Override
    public void resume() {

    }

    @Override
    public void request(long n) {

    }
}