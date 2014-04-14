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
package reactor.core.composable.action;

import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import org.reactivestreams.spi.Subscriber;
import reactor.core.composable.Composable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephane Maldini
 */
public abstract class ActionUtils {

	public static String browseAction(Action action) {
		ActionVisitor actionVisitor = new ActionVisitor(action.getOutput());
		actionVisitor.parseSubscription(action, 0);
		return actionVisitor.toString();
	}


	public static String browseComposable(Composable<?> composable) {
		ActionProcessor<?> actionProcessor = composable.getPublisher();
		ActionVisitor actionVisitor = new ActionVisitor(actionProcessor);
		actionVisitor.drawActionPublisher(actionProcessor);
		return actionVisitor.toString();
	}

	public static class ActionVisitor {

		final private StringBuilder appender;
		final private List<Throwable> errors = new ArrayList<Throwable>();

		private ActionVisitor(ActionProcessor<?> actionProcessor) {
			this.appender = new StringBuilder("\nProcessor[" + actionProcessor.getState() + "]");
		}

		private ActionVisitor drawActionPublisher(ActionProcessor<?> actionProcessor) {
			drawActionPublisher(actionProcessor, 1);
			if (!errors.isEmpty()) {
				System.out.println("\n"+errors.size()+" exception traces (in order of appearance):");
				for (Throwable error : errors) {
					error.printStackTrace();
					System.out.println("\n");
				}
			}
			return this;
		}

		private ActionVisitor drawActionPublisher(ActionProcessor<?> actionProcessor,
		                                          int d) {

			loopRegistredActions(actionProcessor.getSubscriptions(), d);
			return this;
		}

		@SuppressWarnings("unchecked")
		private void parseSubscription(Object subscription, int d) {
			appender.append("\n");
			for (int i = 0; i < d; i++)
				appender.append("|   ");
			appender.append("|____");


			Subscriber<?> operation = null;
			if (ActionProcessor.ActionSubscription.class.isAssignableFrom(subscription.getClass())) {
				operation = ((ActionProcessor.ActionSubscription<?>) subscription).subscriber;
			} else if (Action.class.isAssignableFrom(subscription.getClass())) {
				operation = (Action) subscription;
			}
			if (operation != null) {
				appender.append(operation.getClass().getSimpleName().isEmpty() ? operation.getClass().getName() + "" + operation :
						operation
								.getClass()
								.getSimpleName().replaceAll("Action", "") + "[" + operation + "]");

				renderFilter(operation, d);

				Action action;
				if (Action.class.isAssignableFrom(operation.getClass())) {

					action = (Action) operation;
					if (action.error != null) {
						errors.add(action.error);
						appender.append(" /!\\ - ").append(action.error);
					}
					if (action.pause) {
						appender.append(" (!) - Paused");
					}
					if (Flushable.class.isAssignableFrom(operation.getClass())) {
						appender.append(" <Flushable>");
					}
					if (ActionProcessor.ActionSubscription.class.isAssignableFrom(subscription.getClass())) {
						ActionProcessor.ActionSubscription<?> actionSubscription = (ActionProcessor.ActionSubscription<?>)
								subscription;
						if (actionSubscription.completed) {
							appender.append(" ∑ completed");
						} else if (actionSubscription.error) {
							appender.append(" ∑ error");
						} else if (actionSubscription.cancelled) {
							appender.append(" ∑ cancelled");
						}
					}

					if (action.output != null) {
						drawActionPublisher(
								action.output,
								d + 1
						);
					}
				}
			} else {
				appender.append(subscription.getClass().getSimpleName().isEmpty() ? subscription :
						subscription
								.getClass()
								.getSimpleName() + "[" + subscription + "]");
			}

		}

		private void loopRegistredActions(MutableList<?> operations, final int d) {
			operations.forEach(new CheckedProcedure<Object>() {
				@Override
				public void safeValue(Object registration) throws Exception {
					parseSubscription(registration, d);
				}
			});
		}

		private void renderFilter(Object consumer, int d) {
			if (FilterAction.class.isAssignableFrom(consumer.getClass())) {
				FilterAction<?> operation = (FilterAction<?>) consumer;

				if (operation.getElsePublisher() != null) {
					loopRegistredActions(operation.getElsePublisher().getSubscriptions(), d + 1);
				}
			}
		}


		@Override
		public String toString() {
			return appender.toString();
		}
	}
}
