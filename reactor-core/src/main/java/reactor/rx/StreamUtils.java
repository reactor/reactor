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

import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import reactor.rx.action.Action;
import reactor.rx.action.FilterAction;

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

		private <O> void parseComposable(Stream<O> composable, int d) {
			if (d > 0) {
				appender.append("\n");
				for (int i = 0; i < d; i++)
					appender.append("|   ");
				appender.append("|____");
			}

			appender.append(composable.getClass().getSimpleName().isEmpty() ? composable.getClass().getName() + "" +
					composable :
					composable
							.getClass()
							.getSimpleName().replaceAll("Action", "") + "[" + composable + "]");

			renderFilter(composable, d);

			if (composable.error != null) {
				errors.add(composable.error);
				appender.append(" /!\\ - ").append(composable.error);
			}
			if (composable.pause) {
				appender.append(" (!) - Paused");
			}
			if (Action.class.isAssignableFrom(composable.getClass())
					&& ((Action<?, O>) composable).getSubscription() != null
					&& StreamSubscription.class.isAssignableFrom(((Action<?, O>) composable).getSubscription().getClass())) {

				StreamSubscription<?> composableSubscription =
						(StreamSubscription<O>) ((Action<?, O>) composable).getSubscription();

				if (composableSubscription.completed) {
					appender.append(" ∑ completed");
				} else if (composableSubscription.error) {
					appender.append(" ∑ error");
				} else if (composableSubscription.cancelled) {
					appender.append(" ∑ cancelled");
				}
			}

			loopSubscriptions(
					composable.getSubscriptions(),
					d + 1
			);
		}

		@SuppressWarnings("unchecked")
		private <O> void loopSubscriptions(MutableList<StreamSubscription<O>> operations, final int d) {
			operations.forEach(new CheckedProcedure<StreamSubscription<O>>() {
				@Override
				public void safeValue(StreamSubscription<O> registration) throws Exception {
					if (Stream.class.isAssignableFrom(registration.subscriber.getClass())) {
						parseComposable((Stream<O>) registration.subscriber, d);
					} else {
						appender.append(registration.subscriber.getClass().getSimpleName().isEmpty() ?
								registration.subscriber :
								registration.subscriber
										.getClass()
										.getSimpleName().replaceAll("Action", "") + "[" + registration.subscriber + "]");

					}
				}
			});
		}

		private <O> void renderFilter(Stream<O> consumer, int d) {
			if (FilterAction.class.isAssignableFrom(consumer.getClass())) {
				FilterAction<O, ?> operation = (FilterAction<O, ?>) consumer;

				if (operation.otherwise() != null) {
					if (Stream.class.isAssignableFrom(operation.otherwise().getClass()))
						loopSubscriptions(((Stream<O>) operation.otherwise()).getSubscriptions(), d + 2);
					else if (Promise.class.isAssignableFrom(operation.otherwise().getClass()))
						loopSubscriptions(((Promise<O>) operation.otherwise()).delegateAction.getSubscriptions(), d + 2);
				}
			}
		}


		@Override
		public String toString() {
			return appender.toString();
		}
	}
}
