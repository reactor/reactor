package reactor.alloc;

/**
 * @author Jon Brisbin
 */
public class RecyclableNumber extends Number implements Recyclable {

	private volatile double value = -1;

	public void setValue(int value) {
		this.value = value;
	}

	public void setValue(long value) {
		this.value = value;
	}

	public void setValue(float value) {
		this.value = value;
	}

	public void setValue(double value) {
		this.value = value;
	}

	@Override
	public int intValue() {
		return (int) value;
	}

	@Override
	public long longValue() {
		return (long) value;
	}

	@Override
	public float floatValue() {
		return (float) value;
	}

	@Override
	public double doubleValue() {
		return value;
	}

	@Override
	public void recycle() {
		value = -1;
	}

}
