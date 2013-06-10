package reactor.tcp.encoding.syslog;

import reactor.fn.Consumer;
import reactor.tcp.TcpConnection;

/**
 * Abstract helper class to aid in consuming {@link SyslogMessage SyslogMessages}.
 *
 * @author Jon Brisbin
 */
public abstract class SyslogConsumer implements Consumer<TcpConnection<SyslogMessage, Void>> {

	@Override
	public final void accept(TcpConnection<SyslogMessage, Void> conn) {
		conn.consume(new Consumer<SyslogMessage>() {
			@Override
			public void accept(SyslogMessage syslogMessage) {
				SyslogConsumer.this.accept(syslogMessage);
			}
		});
	}

	/**
	 * Implementations of this class should override this method to act on messages received.
	 *
	 * @param msg The incoming syslog message.
	 */
	protected abstract void accept(SyslogMessage msg);

}
