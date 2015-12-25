package reactor.pipe.state;

import reactor.pipe.concurrent.Atom;

public class DefaultStateProvider<K> implements StateProvider<K> {

    @Override
    public <T> Atom<T> makeAtom(K src, T init) {
        return new Atom<>(init);
    }

}
