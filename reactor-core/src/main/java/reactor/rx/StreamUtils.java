/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.rx;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.function.Consumer;
import reactor.rx.action.*;
import reactor.rx.action.support.GroupedByStream;

import java.io.Serializable;
import java.util.*;

/**
 * A simple collection of utils to assist in various tasks such as Debugging
 *
 * @author Stephane Maldini
 * @since 1.1
 */
public abstract class StreamUtils {

	public static <O> StreamVisitor browse(Stream<O> composable) {
		return browse(composable, new DebugVisitor());
	}

	public static <O> StreamVisitor browse(Stream<O> composable, DebugVisitor visitor) {
		StreamVisitor explorer = new StreamVisitor(visitor);
		explorer.accept(composable);
		return explorer;
	}

	static class DebugVisitor implements Consumer<Stream<?>> {

		final private StringBuilder   appender = new StringBuilder();
		final private List<Throwable> errors   = new ArrayList<Throwable>();
		int d = 0;

		@Override
		public void accept(Stream<?> composable) {
			newLine(d);

			appender.append(composable.getClass().getSimpleName().isEmpty() ? composable.getClass().getName() + "" +
					composable :
					composable
							.getClass()
							.getSimpleName().replaceAll("Action", "") + "[" + composable + "]");


			if (composable.error != null) {
				errors.add(composable.error);
				appender.append(" /!\\ - ").append(composable.error);
			}
			if (composable.pause) {
				appender.append(" (!) - Paused");
			}
		}

		@Override
		public String toString() {
			return appender.toString();
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

	public static class StreamVisitor implements Consumer<Stream<?>> {

		final private Set<Object>         references = new HashSet<Object>();
		final private Map<Object, Object> streamTree = new HashMap<Object, Object>();
		final private DebugVisitor debugVisitor;

		public StreamVisitor(DebugVisitor debugVisitor) {
			this.debugVisitor = debugVisitor;
		}

		@Override
		public void accept(Stream<?> composable) {
			parseComposable(composable, null);
		}

		@SuppressWarnings("unchecked")
		private <O> void parseComposable(Stream<O> composable, final List<Object> streamTree) {
			if(composable == null) return;

			Map<Object, Object> freshNestedStreams = new HashMap<Object, Object>();
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
				debugVisitor.accept(composable);
				debugVisitor.d += 2;
			}

			List<Object> nextLevelNestedStreams = new ArrayList<Object>();

			boolean hasRenderedSpecial =
					renderParallel(composable, nextLevelNestedStreams)
							|| renderWindow(composable, nextLevelNestedStreams)
							|| renderGroupBy(composable, nextLevelNestedStreams)
							|| renderFilter(composable, nextLevelNestedStreams)
							|| renderDynamicMerge(composable, nextLevelNestedStreams)
							|| renderMerge(composable, nextLevelNestedStreams)
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
			Consumer<E> procedure = new Consumer<E>() {
				@Override
				public void accept(E registration){
					if (StreamSubscription.class.isAssignableFrom(registration.getClass())) {
						Subscriber<?> subscriber = ((StreamSubscription<?>) registration).getSubscriber();
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
								wrappedSubscriber.put("state", ((Promise<?>) subscriber).state);
								parseComposable(((Promise<?>) subscriber).outboundStream, wrappedStream);
							} else {
								wrappedSubscriber.put("info", subscriber.toString());
							}
							streamTree.add(wrappedSubscriber);
							if (debugVisitor != null) {
								debugVisitor.newLine(debugVisitor.d, false);
							}
						}
					}
				}
			};

			if (FanOutSubscription.class.isAssignableFrom(operation.getClass())) {
				((FanOutSubscription) operation).forEach(procedure);
			} else {
				procedure.accept(operation);
			}
		}

		private <O> boolean renderFilter(Stream<O> consumer, final List<Object> streamTree) {
			if (FilterAction.class.isAssignableFrom(consumer.getClass())) {
				FilterAction<O, ?> operation = (FilterAction<O, ?>) consumer;

				if (operation.otherwise() != null) {
					if (Stream.class.isAssignableFrom(operation.otherwise().getClass()))
						loopSubscriptions((operation.otherwise()).downstreamSubscription(), streamTree);
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
					loopSubscriptions(operation.currentWindow().downstreamSubscription(), streamTree);
				}
				return true;
			}
			return false;
		}


		@SuppressWarnings("unchecked")
		private <O> boolean renderCombine(Stream<O> consumer, final List<Object> streamTree) {
			if (CombineAction.class.isAssignableFrom(consumer.getClass())) {
				CombineAction<O, ?> operation = (CombineAction<O, ?>) consumer;
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
		private <O> boolean renderParallel(Stream<O> consumer, final List<Object> streamTree) {
			if (ParallelAction.class.isAssignableFrom(consumer.getClass())) {
				ParallelAction<O> operation = (ParallelAction<O>) consumer;
				for (Stream<O> s : operation.getPublishers()) {
					parseComposable(s, streamTree);
					if (debugVisitor != null) {
						debugVisitor.newLine(debugVisitor.d, false);
					}
				}
				return true;
			}
			return false;
		}

		@SuppressWarnings("unchecked")
		private <O> boolean renderGroupBy(Stream<O> consumer, final List<Object> streamTree) {
			if (GroupByAction.class.isAssignableFrom(consumer.getClass())) {
				GroupByAction<O, ?> operation = (GroupByAction<O, ?>) consumer;
				for (Stream<O> s : operation.groupByMap().values()) {
					parseComposable(s, streamTree);
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
				FanInAction<?, O> operation = (FanInAction<?, O>) consumer;
				operation.getSubscription().forEach(new Consumer<FanInSubscription.InnerSubscription>() {
					Subscription delegateSubscription;

					@Override
					public void accept(FanInSubscription.InnerSubscription subscription) {
						delegateSubscription = subscription.getDelegate();
						if (StreamSubscription.class.isAssignableFrom(delegateSubscription.getClass())) {
							Publisher<?> publisher = ((StreamSubscription) delegateSubscription).getPublisher();
							if (references.contains(publisher)) return;
							if (Action.class.isAssignableFrom(publisher.getClass())) {
								parseComposable(((Action<?, ?>) publisher).findOldestAction(false), streamTree);
							} else if (Stream.class.isAssignableFrom(publisher.getClass())) {
								parseComposable((Stream<?>) publisher, streamTree);
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
			if (GroupedByStream.class.isAssignableFrom(stream.getClass())) {
				this.key = ((GroupedByStream) stream).key();
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
