package reactor.pipe.registry;


import reactor.bus.registry.Registration;
import reactor.bus.selector.Selector;

public final class KeyRegistration<K, V> implements Registration<K, V> {

    private final V object;

    public KeyRegistration(V object) {
        this.object = object;
    }

    @Override
    public Selector<K> getSelector() {
        return null;
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
    public Registration<K, V> cancel() {
        return null;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public Registration<K, V> pause() {
        return null;
    }

    @Override
    public boolean isPaused() {
        return false;
    }

    @Override
    public Registration<K, V> resume() {
        return null;
    }

}
