package reactor.pipe.key;

import org.junit.Test;
import reactor.pipe.key.Key;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class KeyTest {

  @Test
  public void isDerivedTest() {
    Key k = Key.wrap("key");

    assertThat(k.isDerived(), is(false));
    assertThat(k.derive().isDerived(), is(true));
    assertThat(k.derive().derive().isDerived(), is(true));
  }

  @Test
  public void isDerivedFromTest() {
    Key k = Key.wrap("key");

    assertThat(k.derive().isDerivedFrom(k), is(true));
    assertThat(k.derive().isDerivedFrom(k.derive()), is(false));
    assertThat(k.derive().isDerivedFrom(Key.wrap("other")), is(false));
    assertThat(k.derive().isDerivedFrom(Key.wrap("key")), is(true));
  }

  @Test
  public void equalityTest() {
    assertThat(Key.wrap("key").equals(Key.wrap("key")), is(true));
    assertThat(Key.wrap("key").equals(Key.wrap("other")), is(false));
    assertThat(Key.wrap("key").equals(Key.wrap("key").derive()), is(false));
  }
}
