/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.bus.registry;

import reactor.bus.selector.ObjectSelector;
import reactor.bus.selector.Selector;
import reactor.core.support.ReactiveState;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class CachableRegistration<K, V> implements Registration<K, V>, ReactiveState.Downstream,
                                                   ReactiveState.Grouped<Selector>,
                                                   ReactiveState.ActiveDownstream,
                                                   ReactiveState.Inner
                                                   {

	private static final Selector<Void> NO_MATCH = new ObjectSelector<Void, Void>(null) {
		@Override
		public boolean matches(Void key) {
			return false;
		}
	};

	private final Selector<K> selector;
	private final V           object;
	private final Runnable    onCancel;
	private final boolean     lifecycle;

	private volatile boolean cancelled      = false;
	private volatile boolean cancelAfterUse = false;
	private volatile boolean paused         = false;

	public CachableRegistration(Selector<K> selector, V object, Runnable onCancel) {
		this.selector = selector;
		this.object = object;
		this.onCancel = onCancel;
		this.lifecycle = Pausable.class.isAssignableFrom(object.getClass());
	}

	@Override
	@SuppressWarnings("unchecked")
	public Selector<K> getSelector() {
		return (!cancelled ? selector : (Selector<K>) NO_MATCH);
	}

	@Override
	public V getObject() {
		return (!cancelled && !paused ? object : null);
	}

	@Override
	public Registration<K, V> cancelAfterUse() {
		this.cancelAfterUse = true;
		return this;
	}

	@Override
	public boolean isCancelAfterUse() {
		return cancelAfterUse;
	}

	@Override
	public void cancel() {
		if (!cancelled) {
			if (null != onCancel) {
				onCancel.run();
			}
			if (lifecycle) {
				((Pausable) object).cancel();
			}
			this.cancelled = true;
		}
	}

	@Override
	public boolean isCancelled() {
		return cancelled;
	}

	@Override
	public void pause() {
		this.paused = true;
		if (lifecycle) {
			((Pausable) object).pause();
		}
	}

	@Override
	public void request(long n){
		//ignore
	}

	@Override
	public boolean isPaused() {
		return paused;
	}

	@Override
	public void resume() {
		paused = false;
		if (lifecycle) {
			((Pausable) object).resume();
		}
	}

	@Override
	public Object downstream() {
		return object;
	}

	@Override
	public Selector key() {
		return selector;
	}

	@Override
	public String toString() {
		return "CachableRegistration{" +
		  "\n\tselector=" + selector +
		  ",\n\tobject=" + object +
		  ",\n\tonCancel=" + onCancel +
		  ",\n\tlifecycle=" + lifecycle +
		  ",\n\tcancelled=" + cancelled +
		  ",\n\tcancelAfterUse=" + cancelAfterUse +
		  ",\n\tpaused=" + paused +
		  "\n}";
	}

}
