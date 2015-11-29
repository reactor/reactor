package reactor.pipe.state;

import reactor.pipe.concurrent.Atom;

public interface StatefulSupplier<ST, T> {

  public T get(Atom<ST> state);

}
