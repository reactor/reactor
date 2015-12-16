package reactor.pipe.registry;


import org.pcollections.HashTreePMap;
import org.pcollections.PMap;
import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registry;
import reactor.bus.selector.Selector;
import reactor.fn.UnaryOperator;
import reactor.pipe.concurrent.Atom;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class ConcurrentRegistry<K, V> implements Registry<K, V> {

    private final Atom<PMap<K, PVector<Registration<K, V>>>> exactKeyMatches;
    // This one can't be map, since key miss matcher is a possibly non-capturing lambda,
    // So we have no other means to work around the uniqueness
    private final List<Registration<K, V>>                   keyMissMatchers;

    public ConcurrentRegistry() {
        this.exactKeyMatches = new Atom<>(HashTreePMap.empty());
        this.keyMissMatchers = new ArrayList<>();
    }

    @Override
    public Registration<K, V> register(Selector<K> matcher, V value) {
        Registration<K, V> registration = new DelayedRegistration<>(matcher, value);
        this.keyMissMatchers.add(registration);
        return registration;
    }

    @Override
    public Registration<K, V> register(K key, V obj) {
        final PVector<Registration<K, V>> lookedUpArr = exactKeyMatches.deref().get(key);
        final Registration<K, V> reg = new KeyRegistration<>(obj);
        if (lookedUpArr == null) {
            exactKeyMatches.update(new UnaryOperator<PMap<K, PVector<Registration<K, V>>>>() {
                @Override
                public PMap<K, PVector<Registration<K, V>>> apply(PMap<K, PVector<Registration<K, V>>> old) {
                    return old.plus(key, TreePVector.singleton(reg));
                }
            });
        } else {
            exactKeyMatches.update(new UnaryOperator<PMap<K, PVector<Registration<K, V>>>>() {
                @Override
                public PMap<K, PVector<Registration<K, V>>> apply(PMap<K, PVector<Registration<K, V>>> old) {
                    return old.plus(key, old.get(key).plus(reg));
                }
            });
        }
        return reg;
    }

    @Override
    public List<Registration<K, ? extends V>> select(K key) {
        PVector<Registration<K, V>> registrations = exactKeyMatches.deref().get(key);
        if (null == registrations || registrations.isEmpty()) {
            return keyMissMatchers.stream().filter((reg) -> {
                return reg.getSelector().matches(key);
            }).collect(Collectors.toList());
        } else {
            return registrations.stream().collect(Collectors.toList());
        }
    }

    @Override
    public boolean unregister(K key) {
        return false;
    }


    @Override
    public Iterable<? extends V> selectValues(K key) {
        return null;
    }

    @Override
    public void clear() {

    }

    @Override
    public Iterator<Registration<K, ? extends V>> iterator() {
        return null;
    }


}
