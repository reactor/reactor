package reactor.pipe.key;

import java.util.Arrays;
import java.util.UUID;

public class Key {

  private final Object[] parts;
  private final boolean isDerived;
  private volatile Object metadata;

  public Key(Object[] parts) {
    this(parts, false, null);
  }

  protected Key(Object[] parts, boolean isDerived, Object metadata) {
    this.parts = parts;
    this.isDerived = isDerived;
    this.metadata = metadata;
  }

  @SuppressWarnings("unchecked")
  public <T> void setMetadata(T metadata) {
    this.metadata = metadata;
  }

  @SuppressWarnings("unchecked")
  public <T> T getMetadata() {
    return (T) metadata;
  }

  public Key derive() {
    Object[] newKey = new Object[parts.length + 1];
    System.arraycopy(parts, 0, newKey, 0, parts.length);
    newKey[parts.length] = UUID.randomUUID();
    return new Key(newKey, true, metadata);
  }

  @SuppressWarnings("unchecked")
  public <T> T getPart(int index) {
    if (index > parts.length - 1) {
      throw new RuntimeException(String.format("Can't get a part with index %d from %d long key",
                                               index,
                                               parts.length));
    } else {
      return (T) parts[index];
    }
  }

  public Key clone(Key metadataSource) {
    Key k = Key.wrap(parts);
    k.setMetadata(metadataSource.getMetadata());
    return k;
  }

  @Override
  public Key clone() {
    return Key.wrap(parts);
  }

  public boolean isDerivedFrom(Key other) {
    if (!isDerived || (this.parts.length <= other.parts.length)) {
      return false;
    }

    for(int i = 0; i < other.parts.length; i++) {
      if (!(other.parts[i].equals(this.parts[i]))) {
        return false;
      }
    }
    return true;
  }

  public boolean isDerived() {
    return isDerived;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    Key key = (Key) o;

    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    if (!Arrays.equals(parts, key.parts))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return parts != null ? Arrays.hashCode(parts) : 0;
  }

  public static Key wrap(Object k) {
    return new Key(new Object[] { k } );
  }

  public static Key wrap(Object... k) {
    return new Key(k);
  }

  @Override
  public String toString() {
    return "Key{" +
           "parts=" + Arrays.toString(parts) +
           '}';
  }

  public Object[] getObjects() {
    return Arrays.copyOf(parts, parts.length);
  }
}

