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
package reactor.rx;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.Publishable;
import reactor.core.support.Subscribable;
import reactor.fn.Consumer;
import reactor.rx.action.CompositeAction;
import reactor.rx.action.aggregation.WindowAction;
import reactor.rx.action.combination.DynamicMergeAction;
import reactor.rx.action.combination.FanInAction;
import reactor.rx.action.combination.FanInSubscription;
import reactor.rx.action.combination.SwitchAction;
import reactor.rx.action.error.RetryWhenAction;
import reactor.rx.action.transformation.GroupByAction;
import reactor.rx.stream.GroupedStream;
import reactor.rx.subscription.FanOutSubscription;
import reactor.rx.subscription.PushSubscription;

import java.io.Serializable;
import java.util.*;

/**
 * A simple collection of utils to assist in various tasks such as Debugging
 *
 * @author Stephane Maldini
 * @since 1.1
 */
public abstract class StreamUtils {

	public static <O> StreamVisitor browse(Publishable<O> composable) {
		return browse(composable, new DebugVisitor());
	}

	public static <O> StreamVisitor browse(Publishable<O> composable, DebugVisitor visitor) {
		StreamVisitor explorer = new StreamVisitor(visitor);
		explorer.accept(composable);
		return explorer;
	}

	static class DebugVisitor implements Consumer<Publishable<?>> {

		final private StringBuilder   appender = new StringBuilder();
		final private List<Throwable> errors   = new ArrayList<Throwable>();
		int d = 0;

		@Override
		public void accept(Publishable<?> composable) {
			newLine(d);

			appender.append(composable.getClass().getSimpleName().isEmpty() ? composable.getClass().getName() + "" +
			  composable :
			  composable
				.getClass()
				.getSimpleName().replaceAll("Action", "") + "[" + composable + "]");
		}

		@Override
		public String toString() {
			return appender.toString();
		}

		public void newMulticastLine(int d) {
			appender.append("\n");
			for (int i = 0; i < d + 1; i++)
				appender.append("|   ");
		}

		private void newLine(int d) {
			newLine(d, true);
		}

		private void newLine(int d, boolean prefix) {
			appender.append("\n");
			for (int i = 0; i < d; i++)
				appender.append("|   ");
			if (prefix) appender.append("|____");
		}
	}

	public static class StreamVisitor implements Consumer<Publishable<?>> {

		final private Set<Object>         references = new HashSet<Object>();
		final private Map<Object, Object> streamTree = new HashMap<Object, Object>();
		final private DebugVisitor debugVisitor;

		public StreamVisitor(DebugVisitor debugVisitor) {
			this.debugVisitor = debugVisitor;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void accept(Publishable<?> composable) {
			if (composable == null) return;

			Publisher upstream = null;
			Publishable<?> next = composable;
			while (next != null) {
				upstream = next.upstream();
				if (upstream != null) {
					if (Publishable.class.isAssignableFrom(upstream.getClass())) {
						next = (Publishable) upstream;
						continue;
					}
				}
				next = null;
			}
			if (upstream != null && upstream != composable) {
				if (debugVisitor != null) {
					debugVisitor.newLine(debugVisitor.d);
					debugVisitor.appender.append(upstream);
				}
			}
			if (Stream.class.isAssignableFrom(composable.getClass())) {
				parseComposable((Stream) composable, null);
			}
		}

		@SuppressWarnings("unchecked")
		private <O> void parseComposable(Stream<O> composable, final List<Object> streamTree) {
			if (composable == null) return;

			Map<Object, Object> freshNestedStreams = new HashMap<>();
			freshNestedStreams.put("id", new StreamKey(composable).toString());

			if (streamTree != null) {
				streamTree.add(freshNestedStreams);
			}

			if (references.contains(composable)) {
				return;
			}

			freshNestedStreams.put("info", composable.toString());
			references.add(composable);

			if (debugVisitor != null) {
				if(Publishable.class.isAssignableFrom(composable.getClass())) {
					debugVisitor.accept((Publishable) composable);
				}else{
					debugVisitor.newLine(debugVisitor.d);
					debugVisitor.appender.append(composable);
				}
				debugVisitor.d += 2;
			}

			List<Object> nextLevelNestedStreams = new ArrayList<Object>();

			boolean hasRenderedSpecial =
			  renderWindow(composable, nextLevelNestedStreams)
				|| renderGroupBy(composable, nextLevelNestedStreams)
				|| renderSwitch(composable, nextLevelNestedStreams)
				|| renderDynamicMerge(composable, nextLevelNestedStreams)
				|| renderMerge(composable, nextLevelNestedStreams)
				|| renderRetryWhen(composable, nextLevelNestedStreams)
				|| renderCombine(composable, nextLevelNestedStreams);

			if (!nextLevelNestedStreams.isEmpty()) {
				freshNestedStreams.put("boundTo", nextLevelNestedStreams);
			}


			if (debugVisitor != null) {
				debugVisitor.d -= 2;
			}

			nextLevelNestedStreams = new ArrayList<Object>();
			loopSubscriptions(
			  composable.downstreamSubscription(),
			  nextLevelNestedStreams
			);

			if (!nextLevelNestedStreams.isEmpty()) {
				freshNestedStreams.put("to", nextLevelNestedStreams);
			}

			if (streamTree == null) {
				this.streamTree.putAll(freshNestedStreams);
			}

		}

		@SuppressWarnings("unchecked")
		private <E extends Subscription> void loopSubscriptions(E operation, final List<Object> streamTree) {
			if (operation == null) return;

			final boolean multicast = FanOutSubscription.class.isAssignableFrom(operation.getClass());

			Consumer<E> procedure = new Consumer<E>() {
				@Override
				public void accept(E registration) {
					Subscriber<?> subscriber = null;
					if (PushSubscription.class.isAssignableFrom(registration.getClass())) {
						subscriber = ((PushSubscription<?>) registration).getSubscriber();
					}
					Subscriber<?> unproxy = subscriber;
					while (unproxy != null && Subscribable.class.isAssignableFrom(unproxy.getClass())) {
						subscriber = unproxy;
						if (debugVisitor != null) {
							debugVisitor.newLine(debugVisitor.d);
							debugVisitor.appender.append(subscriber).append(registration);
						}
						unproxy = ((Subscribable<?>) unproxy).downstream();
					}

					if (unproxy != null) {
						subscriber = unproxy;
					}

					if (subscriber != null) {
						if (debugVisitor != null && multicast) {
							debugVisitor.d++;
							debugVisitor.newMulticastLine(debugVisitor.d);
						}

						if (Stream.class.isAssignableFrom(subscriber.getClass())) {
							parseComposable((Stream<?>) subscriber, streamTree);
						} else {
							Map<Object, Object> wrappedSubscriber = new HashMap<Object, Object>();
							if (debugVisitor != null) {
								debugVisitor.newLine(debugVisitor.d);
								debugVisitor.appender.append(subscriber).append(registration);
							}
							if (Promise.class.isAssignableFrom(subscriber.getClass())) {
								List<Object> wrappedStream = new ArrayList<Object>();
								wrappedSubscriber.put("info", subscriber.toString());

								if (((Promise<?>) subscriber).finalState != null)
									wrappedSubscriber.put("state", ((Promise<?>) subscriber).finalState);

								parseComposable(((Promise<?>) subscriber).outboundStream, wrappedStream);
							} else {
								wrappedSubscriber.put("info", subscriber.toString());
							}
							streamTree.add(wrappedSubscriber);
						}
						if (debugVisitor != null && multicast) {
							debugVisitor.d--;
							debugVisitor.newLine(debugVisitor.d, false);
						}
					}
				}
			};

			if (multicast) {
				((FanOutSubscription) operation).forEach(procedure);
			} else {
				procedure.accept(operation);
			}
		}


		private <O> boolean renderSwitch(Stream<O> consumer, final List<Object> streamTree) {
			if (SwitchAction.class.isAssignableFrom(consumer.getClass())) {
				SwitchAction<O> operation = (SwitchAction<O>) consumer;
				SwitchAction.SwitchSubscriber switchSubscriber = operation.getSwitchSubscriber();
				if (switchSubscriber != null && switchSubscriber.getSubscription() != null) {
					loopSubscriptions(switchSubscriber.getSubscription(), streamTree);
				}
				return true;
			}
			return false;
		}

		@SuppressWarnings("unchecked")
		private <O> boolean renderWindow(Stream<O> consumer, final List<Object> streamTree) {
			if (WindowAction.class.isAssignableFrom(consumer.getClass())) {
				WindowAction<O> operation = (WindowAction<O>) consumer;

				if (operation.currentWindow() != null) {
					loopSubscriptions(operation.currentWindow(), streamTree);
				}
				return true;
			}
			return false;
		}


		@SuppressWarnings("unchecked")
		private <O> boolean renderCombine(Stream<O> consumer, final List<Object> streamTree) {
			if (CompositeAction.class.isAssignableFrom(consumer.getClass())) {
				CompositeAction<O, ?> operation = (CompositeAction<O, ?>) consumer;
				parseComposable(operation.input(), streamTree);
				return true;
			}
			return false;
		}

		@SuppressWarnings("unchecked")
		private <O> boolean renderDynamicMerge(Stream<O> consumer, final List<Object> streamTree) {
			if (DynamicMergeAction.class.isAssignableFrom(consumer.getClass())) {
				DynamicMergeAction<?, O> operation = (DynamicMergeAction<?, O>) consumer;
				parseComposable(operation.mergedStream(), streamTree);
				return true;
			}
			return false;
		}

		@SuppressWarnings("unchecked")
		private <O> boolean renderRetryWhen(Stream<O> consumer, final List<Object> streamTree) {
			if (RetryWhenAction.class.isAssignableFrom(consumer.getClass())) {
				RetryWhenAction<O> operation = (RetryWhenAction<O>) consumer;
				parseComposable(operation.retryStream(), streamTree);
				return true;
			}
			return false;
		}

		@SuppressWarnings("unchecked")
		private <O> boolean renderGroupBy(Stream<O> consumer, final List<Object> streamTree) {
			if (GroupByAction.class.isAssignableFrom(consumer.getClass())) {
				GroupByAction<O, ?> operation = (GroupByAction<O, ?>) consumer;
				for (PushSubscription<O> s : operation.groupByMap().values()) {
					loopSubscriptions(s, streamTree);
					if (debugVisitor != null) {
						debugVisitor.newLine(debugVisitor.d, false);
					}
				}
				return true;
			}
			return false;
		}

		@SuppressWarnings("unchecked")
		private <O> boolean renderMerge(Stream<O> consumer, final List<Object> streamTree) {
			if (FanInAction.class.isAssignableFrom(consumer.getClass())) {
				FanInAction operation = (FanInAction) consumer;
				operation.getSubscription().forEach(
				  new Consumer<FanInSubscription.InnerSubscription>() {
					  Subscription delegateSubscription;

					  @Override
					  public void accept(FanInSubscription.InnerSubscription subscription) {
						  delegateSubscription = subscription.getDelegate();
						  if (Publishable.class.isAssignableFrom(delegateSubscription.getClass())) {
							  Publisher<?> publisher = ((Publishable) delegateSubscription).upstream();
							  if (publisher == null || references.contains(publisher)) return;
							  if (Stream.class.isAssignableFrom(publisher.getClass())) {
								  parseComposable((Stream<?>) publisher, streamTree);
							  } else if (Publishable.class.isAssignableFrom(publisher.getClass())) {
								  StreamVisitor.this.accept((Publishable) publisher);
							  }
						  }
					  }
				  });
				return true;
			}
			return false;
		}

		public Map<Object, Object> toMap() {
			return streamTree;
		}

		@Override
		public String toString() {
			return debugVisitor.toString();
		}
	}

	private final static class StreamKey implements Serializable {

		private final Stream<?> stream;
		private final Object    key;

		public StreamKey(Stream<?> composable) {
			this.stream = composable;
			if (GroupedStream.class.isAssignableFrom(stream.getClass())) {
				this.key = ((GroupedStream) stream).key();
			} else {
				this.key = composable.getClass().getSimpleName().isEmpty() ? composable.getClass().getName() + "" +
				  composable :
				  composable
					.getClass()
					.getSimpleName().replaceAll("Action", "");
			}
		}

		@Override
		public String toString() {
			return key.toString();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			StreamKey streamKey = (StreamKey) o;

			if (!stream.equals(streamKey.stream)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return stream.hashCode();
		}
	}
}
