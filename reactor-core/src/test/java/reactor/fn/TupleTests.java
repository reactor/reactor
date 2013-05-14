package reactor.fn;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author Jon Brisbin
 */
public class TupleTests {

	@Test
	public void tupleHoldsArbitraryValues() {
		Tuple t = Tuple.from("Hello World!");

		assertThat("value is stored as an Object", t.get(0), is((Object) "Hello World!"));
		assertThat("value is a String", String.class.isInstance(t.get(0)));
	}

	@Test
	public void tupleProvidesTypeSafeMethods() {
		Tuple3<String, Long, Integer> t3 = Tuple.from("string", 1L, 10);

		assertThat("first value is a string", String.class.isInstance(t3.getT1()));
		assertThat("second value is a long", Long.class.isInstance(t3.getT2()));
		assertThat("third value is an int", Integer.class.isInstance(t3.getT3()));
	}

	@Test
	public void tupleProvidesTupleTypeHierarchy() {
		Tuple3<String, Long, Integer> t3 = Tuple.from("string", 1L, 10);

		assertThat("Tuple3 is also a Tuple2", Tuple2.class.isInstance(t3));
		assertThat("Tuple3 is also a Tuple1", Tuple1.class.isInstance(t3));
	}

	@Test
	public void tupleProvidesVirtuallyUnlimitedSize() {
		TupleN tn = Tuple.from(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

		assertThat("remaining values are in a Tuple", Tuple.class.isInstance(tn.getTRest()));
		assertThat("last value in TRest is 15", (Integer) tn.getTRest().get(6), is(15));
	}

}
