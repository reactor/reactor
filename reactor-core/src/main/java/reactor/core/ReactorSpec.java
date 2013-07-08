package reactor.core;

/**
 * @author Jon Brisbin
 */
public class ReactorSpec extends EventRoutingComponentSpec<ReactorSpec, Reactor> {

	private boolean link = false;

	public ReactorSpec link() {
		this.link = true;
		return this;
	}

	@Override
	protected Reactor configure(Reactor reactor) {
		if (link && null != this.reactor) {
			this.reactor.link(reactor);
		}
		return reactor;
	}

}
