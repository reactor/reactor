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

import reactor.core.composable.Composable;

/**
 * @author Stephane Maldini
 */
public abstract class ActionUtils {

	public static String browseAction(Action action) {
		ActionVisitor actionVisitor = new ActionVisitor(action.getOutput(), true);
		actionVisitor.parseAction(action, 0, "next/complete");
		return actionVisitor.toString();
	}


	public static String browseComposable(Composable<?> composable) {
		ActionProcessor<?> actionProcessor = composable.getPublisher();
		ActionVisitor actionVisitor = new ActionVisitor(actionProcessor, true);
		actionVisitor.drawActionPublisher(actionProcessor, 1);
		return actionVisitor.toString();
	}

	public static class ActionVisitor {

		final private boolean       visitFailures;
		final private StringBuilder appender;

		private ActionVisitor(ActionProcessor<?> actionProcessor, boolean visitFailures) {
			this.appender = new StringBuilder("\nActionPublisher(" + actionProcessor.toString() + ")");
			this.visitFailures = visitFailures;
		}

		private ActionVisitor drawActionPublisher(ActionProcessor<?> actionProcessor,
		                                          int d) {

			loopRegistredActions(actionProcessor.getSubscriptions(), d, "subscribers");
			loopRegistredActions(actionProcessor.getFlushables(), d, "flush");

			return this;
		}

		private void parseAction(Object action, int d, String marker) {
			appender.append("\n");
			for (int i = 0; i < d; i++)
				appender.append("|   ");
			appender.append("|____" + marker + ":");

			appender.append(action.getClass().getSimpleName().isEmpty() ? action :
					action
							.getClass()
							.getSimpleName().replaceAll("Action", "") + "[" + action + "]");

			if (Action.class.isAssignableFrom(action.getClass())) {
				Action<?,?>	operation = ((Action) action);

				renderBatch(operation, d);
				renderFilter(operation, d);

				drawActionPublisher(
						operation.getOutput(),
						d + 1
				);
			}
		}

		private void loopRegistredActions(Iterable<?> operations, int d,
		                                  String marker) {
			for (Object registration : operations) {
				parseAction(registration, d, marker);
			}
		}

		private void renderFilter(Object consumer, int d) {
			if (FilterAction.class.isAssignableFrom(consumer.getClass())) {
				FilterAction operation = (FilterAction) consumer;

				if (operation.getElsePublisher() != null) {
					loopRegistredActions(operation.getElsePublisher().getSubscriptions()
							,d + 1, "else");
				}
			}
		}


		private void renderBatch(Object consumer, int d) {
			if (BatchAction.class.isAssignableFrom(consumer.getClass())) {
				BatchAction operation = (BatchAction) consumer;
				loopRegistredActions(operation.getOutput().getFlushables(),
						d + 1, "flush");
			}
		}

		@Override
		public String toString() {
			return appender.toString();
		}
	}
}
