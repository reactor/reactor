package reactor.alloc;

/**
 * @author Jon Brisbin
 */
public class RecyclableString implements Recyclable {

	private volatile String value = "";

	public void setValue(String value) {
		this.value = (null == value ? "" : value);
	}

	@Override
	public void recycle() {
		this.value = "";
	}

	@Override
	public String toString() {
		return value;
	}

}
