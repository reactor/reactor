package reactor.tcp.encoding;

import reactor.io.Buffer;

/**
 * @author Jon Brisbin
 */
public class Frame {

	private final Buffer prefix;
	private final Buffer data;

	public Frame(Buffer prefix, Buffer data) {
		this.prefix = prefix;
		this.data = data;
	}

	public Buffer getPrefix() {
		return prefix;
	}

	public Buffer getData() {
		return data;
	}

}
