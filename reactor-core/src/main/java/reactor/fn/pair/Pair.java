package reactor.fn.pair;

/**
 * A {@literal Pair} pair of objects, each of which can be of an arbitrary type.
 *
 * @author Oleksandr Petrov
 */
public class Pair<K, V> {

  private final K first;
  private final V second;

  /**
   * Create a {@code Tap}.
   * @param first the first object in the {@literal Pair}
   * @param second the second object in the {@literal Pair}
   */
  public Pair(K first,
              V second) {
    this.first = first;
    this.second = second;
  }

  /**
   * Get the value of the first object in the {@literal Pair}
   * @return the first object
   */
  public K getFirst() {
    return first;
  }

  /**
   * Get the value of the second object in the {@literal Pair}
   * @return the second object
   */
  public V getSecond() {
    return second;
  }
}
