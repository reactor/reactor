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

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.action.*;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple collection of utils to assist in various tasks such as Debugging
 *
 * @author Stephane Maldini
 * @since 1.1
 */
public abstract class StreamUtils {

	public static <O> String browse(Stream<O> composable) {
		ComposableVisitor composableVisitor = new ComposableVisitor(composable);
		composableVisitor.drawComposablePublisher(composable);
		return composableVisitor.toString();
	}

	public static <O> String browse(Promise<O> composable) {
		ComposableVisitor composableVisitor = new ComposableVisitor(composable.delegateAction);
		composableVisitor.drawComposablePublisher(composable.delegateAction);
		return composableVisitor.toString();
	}

	protected static class ComposableVisitor {

		final private StringBuilder appender;
		final private List<Throwable> errors = new ArrayList<Throwable>();

		private <O> ComposableVisitor(Stream<O> composable) {
			this.appender = new StringBuilder();
		}

		private <O> ComposableVisitor drawComposablePublisher(Stream<O> composableProcessor) {
			parseComposable(composableProcessor, 0);
			if (!errors.isEmpty()) {
				System.out.println("\n" + errors.size() + " exception traces (in order of appearance):");
				for (Throwable error : errors) {
					error.printStackTrace();
					System.out.println("\n");
				}
			}
			return this;
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

		@SuppressWarnings("unchecked")
		private <O> void parseComposable(Stream<O> composable, int d) {
			newLine(d);

			appender.append(composable.getClass().getSimpleName().isEmpty() ? composable.getClass().getName() + "" +
					composable :
					composable
							.getClass()
							.getSimpleName().replaceAll("Action", "") + "[" + composable + "]");


			renderFilter(composable, d);
			renderDynamicMerge(composable, d);
			renderMerge(composable, d);
			renderCombine(composable, d);

			if (composable.error != null) {
				errors.add(composable.error);
				appender.append(" /!\\ - ").append(composable.error);
			}
			if (composable.pause) {
				appender.append(" (!) - Paused");
			}

			loopSubscriptions(
					composable.downstreamSubscription(),
					d
			);
		}

		@SuppressWarnings("unchecked")
		private <E extends Subscription> void loopSubscriptions(E operation, final int d) {
			if (operation == null) return;
			Procedure<E> procedure = new CheckedProcedure<E>() {
				@Override
				public void safeValue(E registration) throws Exception {
					if (StreamSubscription.class.isAssignableFrom(registration.getClass())) {
						Subscriber<?> subscriber = ((StreamSubscription<?>) registration).getSubscriber();
						if (Stream.class.isAssignableFrom(subscriber.getClass())) {
							parseComposable((Stream<?>) subscriber, d);
						} else if (Promise.class.isAssignableFrom(subscriber.getClass())) {
							parseComposable(((Promise<?>) subscriber).delegateAction, d);
						} else {
							newLine(d);
							appender.append("Subscriber[").append(subscriber).append(", ").append(registration).append("]");
						}
					} else {
						newLine(d);
						appender.append("Subscription[").append(registration).append("]");
					}

				}
			};

			if (FanOutSubscription.class.isAssignableFrom(operation.getClass())) {
				((FanOutSubscription) operation).getSubscriptions().forEach(procedure);
			}else{
				procedure.value(operation);
			}
		}

		private <O> void renderFilter(Stream<O> consumer, int d) {
			if (FilterAction.class.isAssignableFrom(consumer.getClass())) {
				FilterAction<O, ?> operation = (FilterAction<O, ?>) consumer;

				if (operation.otherwise() != null) {
					if (Stream.class.isAssignableFrom(operation.otherwise().getClass()))
						loopSubscriptions(((Stream<O>) operation.otherwise()).downstreamSubscription(), d + 2);
					else if (Promise.class.isAssignableFrom(operation.otherwise().getClass()))
						loopSubscriptions(((Promise<O>) operation.otherwise()).delegateAction.downstreamSubscription(), d + 2);
				}
			}
		}


		@SuppressWarnings("unchecked")
		private <O> void renderCombine(Stream<O> consumer, int d) {
			if (CombineAction.class.isAssignableFrom(consumer.getClass())) {
				CombineAction<O, ?, ?> operation = (CombineAction<O, ?, ?>) consumer;
				parseComposable(operation.input(), d + 2);
			}
		}

		@SuppressWarnings("unchecked")
		private <O> void renderDynamicMerge(Stream<O> consumer, int d) {
			if (DynamicMergeAction.class.isAssignableFrom(consumer.getClass())) {
				DynamicMergeAction<O, ?, Publisher<?>> operation = (DynamicMergeAction<O, ?, Publisher<?>>) consumer;
				parseComposable(operation.mergedStream(), d);
			}
		}

		@SuppressWarnings("unchecked")
		private <O> void renderMerge(Stream<O> consumer, final int d) {
			if (MergeAction.class.isAssignableFrom(consumer.getClass())) {
				MergeAction<O> operation = (MergeAction<O>) consumer;
				for (Subscription subscription : operation.getInnerSubscriptions())
					if (StreamSubscription.class.isAssignableFrom(subscription.getClass())) {
						Publisher<?> publisher = ((StreamSubscription<?>) subscription).getPublisher();
						if (Action.class.isAssignableFrom(publisher.getClass())) {
							parseComposable(((Action<?, ?>) publisher).findOldestStream(false), d + 2);
						} else if (Stream.class.isAssignableFrom(publisher.getClass())) {
							parseComposable((Stream<?>) publisher, d + 2);
						} else {
							newLine(d + 2);
							appender.append("Subscriber[").append(publisher).append(", ").append(subscription).append("]");
						}
						newLine(d + 2, false);
					}
			}
		}


		@Override
		public String toString() {
			return appender.toString();
		}
	}
}
