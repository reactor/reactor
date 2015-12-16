package reactor.pipe.state;

import reactor.pipe.concurrent.Atom;

public interface StateProvider<K> {

    public <T> Atom<T> makeAtom(K src, T init);

    public <T> Atom<T> makeAtom(T init);

}
