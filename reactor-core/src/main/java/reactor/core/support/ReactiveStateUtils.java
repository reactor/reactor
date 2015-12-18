/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.support;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class ReactiveStateUtils implements ReactiveState {

	/**
	 * Create an empty graph
	 * @return a Graph
	 */
	public static Graph newGraph() {
		return newGraph(false);
	}

	/**
	 *
	 * @param trace
	 * @return
	 */
	public static Graph newGraph(boolean trace) {
		return new Graph(false, trace);
	}

	/**
	 * Create a "Nodes" and "Links" complete representation of a given component if available
	 * @return a Graph
	 */
	public static Graph scan(Object o) {
		return scan(o, false);
	}

	/**
	 * Create a "Nodes" and "Links" complete representation of a given component if available
	 * @return a Graph
	 */
	public static Graph scan(Object o, boolean trace) {
		if (o == null) {
			return null;
		}
		if (Graph.class.equals(o.getClass())) {
			return (Graph) o;
		}

		Graph graph = new Graph(false, trace);
		Node origin = graph.expandReactiveSate(o, true);
		graph.addUpstream(origin, null);
		graph.addDownstream(origin, null);

		return graph;
	}

	/**
	 * Create a "Nodes" and "Links" downstream representation of a given component if available
	 * @return a Graph
	 */
	public static Graph subscan(Object o) {
		return subscan(o, false);
	}

	/**
	 * Create a "Nodes" and "Links" downstream representation of a given component if available
	 * @return a Graph
	 */
	public static Graph subscan(Object o, boolean trace) {
		if (o == null) {
			return null;
		}

		if (Graph.class.equals(o.getClass())) {
			return (Graph) o;
		}

		Graph graph = new Graph(true, trace);
		Node root = graph.expandReactiveSate(o);
		graph.addDownstream(root, null);

		return graph;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static String prettyPrint(Object o) {
		return print(o, true);
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static String print(Object o) {
		return print(o, false);
	}

	/**
	 *
	 * @param o
	 * @param prettyPrint
	 * @return
	 */
	public static String print(Object o, boolean prettyPrint) {
		if (o == null) {
			return null;
		}
		Node n = new Node(getName(o), getIdOrDefault(o), o, true);
		if (prettyPrint) {
			return n.toPrettyString();
		}
		else {
			return n.toString();
		}
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasUpstream(Object o) {
		return o != null && Upstream.class.isAssignableFrom(o.getClass()) && ((Upstream) o).upstream() != null;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasUpstreams(Object o) {
		return o != null && LinkedUpstreams.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasDownstream(Object o) {
		return o != null && Downstream.class.isAssignableFrom(o.getClass()) && ((Downstream) o).downstream() != null;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasDownstreams(Object o) {
		return o != null && LinkedDownstreams.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasFeedbackLoop(Object o) {
		return o != null && FeedbackLoop.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean isTraceOnly(Object o) {
		return o != null && Trace.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasSubscription(Object o) {
		return o != null && ActiveUpstream.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean isCancellable(Object o) {
		return o != null && ActiveDownstream.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean isContained(Object o) {
		return o != null && Inner.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static long getCapacity(Object o) {
		if (o != null && Bounded.class.isAssignableFrom(o.getClass())) {
			return ((Bounded) o).getCapacity();
		}
		return -1L;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static Throwable getFailedState(Object o) {
		if (o != null && FailState.class.isAssignableFrom(o.getClass())) {
			return ((FailState) o).getError();
		}
		return null;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static long getTimedPeriod(Object o) {
		if (o != null && Timed.class.isAssignableFrom(o.getClass())) {
			return ((Timed) o).period();
		}
		return -1;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static long getUpstreamLimit(Object o) {
		if (o != null && UpstreamPrefetch.class.isAssignableFrom(o.getClass())) {
			return ((UpstreamPrefetch) o).limit();
		}
		return -1L;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static long getExpectedUpstream(Object o) {
		if (o != null && UpstreamDemand.class.isAssignableFrom(o.getClass())) {
			return ((UpstreamDemand) o).expectedFromUpstream();
		}
		return -1L;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static long getRequestedDownstream(Object o) {
		if (o != null && DownstreamDemand.class.isAssignableFrom(o.getClass())) {
			return ((DownstreamDemand) o).requestedFromDownstream();
		}
		return -1L;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static String getName(Object o) {
		if (o == null) {
			return null;
		}

		String name = Named.class.isAssignableFrom(o.getClass()) ? (((Named) o).getName()) : (o.getClass()
		                                                                                       .getSimpleName()
		                                                                                       .isEmpty() ?
				o.toString() : o.getClass()
				                .getSimpleName());

		return name.isEmpty() ? "anonymous" : name;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static String getGroup(Object o) {
		if (o == null) {
			return null;
		}

		Object key = Grouped.class.isAssignableFrom(o.getClass()) ? (((Grouped) o).key()) : null;

		if (key == null) {
			return null;
		}

		return key.toString();
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static String getIdOrDefault(Object o) {
		if(Identified.class.isAssignableFrom(o.getClass())){
			return ((Identified) o).getId();
		}
		return getName(o).hashCode() + ":" + o.hashCode();
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean isUnique(Object o) {
		return o != null && Identified.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean isFactory(Object o) {
		return o != null && Factory.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static long getBuffered(Object o) {
		if (o != null && Buffering.class.isAssignableFrom(o.getClass())) {
			return ((Buffering) o).pending();
		}
		return -1L;
	}

	/**
	 *
	 */
	public static final class Graph {

		private final Map<String, Node> nodes = new HashMap<>();
		private final Map<String, Edge> edges = new WeakHashMap<>();

		private final boolean subscan;
		private final boolean trace;

		private boolean cyclic;

		Graph() {
			this(false, false);
		}

		Graph(boolean subscan, boolean trace) {
			this.subscan = subscan;
			this.trace = trace;
		}

		/**
		 *
		 * @param graph
		 * @return
		 */
		public Graph mergeWith(Graph graph) {
			if (graph == null || (graph.nodes.isEmpty() && graph.edges.isEmpty())) {
				return this;
			}
			nodes.putAll(graph.nodes);
			edges.putAll(graph.edges);
			return this;
		}

		/**
		 * @return a json array of terminated ids
		 */
		public Collection<String> removeTerminatedNodes() {
			if (nodes.isEmpty()) {
				return null;
			}
			Set<String> removedGraph = new HashSet<>();

			Iterator<Node> nodeIterator = nodes.values()
			                                   .iterator();
			Node node;
			Boolean bool1;
			boolean remove;
			while (nodeIterator.hasNext()) {
				node = nodeIterator.next();

				if (node.isReference()) {
					Node n;
					remove = true;
					for (Edge edge : node.connectionsRef) {
						n = nodes.get(edge.from);
						if (n == null) {
							continue;
						}
						bool1 = n.isCancelled();
						if (bool1 == null || !bool1) {
							remove = false;
							break;
						}
						bool1 = n.isTerminated();
						if (bool1 == null || !bool1) {
							remove = false;
							break;
						}
					}
				}
				else {
					bool1 = node.isTerminated();
					remove = bool1 != null && bool1;

					if (!remove) {
						bool1 = node.isCancelled();
						remove = bool1 != null && bool1;
					}

					if (!remove) {
						remove = node.connectionsRef != null && node.connectionsRef.length == 0;
					}
				}
				if (remove) {
					nodeIterator.remove();
					removedGraph.add("\"" + node.getId() + "\"");
				}
			}

			return removedGraph;
		}

		/**
		 *
		 * @param o
		 * @return
		 */
		public Node removeNode(Object o) {
			if (o == null) {
				return null;
			}
			return nodes.remove(getIdOrDefault(o));
		}

		public Collection<Node> getNodes() {
			return nodes.values();
		}

		public Collection<Edge> getEdges() {
			return edges.values();
		}

		public boolean isCyclic() {
			return cyclic;
		}

		private void addUpstream(Node target, Node grandchild) {
			if (target == null) {
				return;
			}
			Node child;
			if (trace || !isTraceOnly(target.object)) {
				child = target;
				if (nodes.containsKey(child.getId()) && grandchild != null) {
					cyclic = true;
					return;
				}
				nodes.put(child.getId(), child);
			}
			else {
				child = grandchild;
			}
			if (hasUpstream(target.object)) {
				Object in = ((Upstream) target.object).upstream();
				if (!virtualRef(in, target)) {
					Node upstream = expandReactiveSate(in, child == null);
					if (child != null && (trace || !isTraceOnly(upstream.object))) {
						addEdge(upstream.createEdgeTo(child));
					}
					addUpstream(upstream, child);
				}
			}
			if (hasUpstreams(target.object)) {
				addUpstreams(child, ((LinkedUpstreams) target.object).upstreams());
			}
			if (hasDownstreams(target.object)) {
				addDownstreams(child, ((LinkedDownstreams) target.object).downstreams());
			}
		}

		private void addUpstreams(Node target, Iterator o) {
			if (o == null) {
				return;
			}
			Node source;
			Object in;
			while (o.hasNext()) {
				in = o.next();
				if (virtualRef(in, target)) {
					continue;
				}
				source = expandReactiveSate(in);
				if (target != null && source != null) {
					addEdge(source.createEdgeTo(target, Edge.Type.inner));
				}
				addUpstream(source, target);
			}
		}

		private void addDownstream(Node origin, Node ancestor) {
			if (origin == null) {
				return;
			}
			Node root;
			if (trace || !isTraceOnly(origin.object)) {
				root = origin;
				if (nodes.containsKey(root.getId()) && ancestor != null) {
					cyclic = true;
					return;
				}
				nodes.put(root.getId(), root);
			}
			else {
				root = ancestor;
			}
			if (hasDownstream(origin.object)) {
				Object out = ((Downstream) origin.object).downstream();
				if (!virtualRef(out, origin)) {
					Node downstream = expandReactiveSate(out, root == null);
					if (root != null && (trace || !isTraceOnly(downstream.object))) {
						addEdge(root.createEdgeTo(downstream));
					}
					addDownstream(downstream, root);
				}
			}
			if (hasDownstreams(origin.object)) {
				addDownstreams(root, ((LinkedDownstreams) origin.object).downstreams());
			}

			if (hasUpstreams(origin.object)) {
				addUpstreams(root, ((LinkedUpstreams) origin.object).upstreams());
			}
		}

		private void addDownstreams(Node source, Iterator o) {
			if (o == null) {
				return;
			}
			Node downstream;
			Object out;
			while (o.hasNext()) {
				out = o.next();
				if (virtualRef(out, source)) {
					continue;
				}
				downstream = expandReactiveSate(out);
				if (source != null && downstream != null) {
					addEdge(source.createEdgeTo(downstream, Edge.Type.inner));
				}
				addDownstream(downstream, source);
			}
		}

		private Node expandReactiveSate(Object o) {
			return expandReactiveSate(o, false);
		}

		private Node expandReactiveSate(Object o, boolean highlight) {
			if (o == null) {
				return null;
			}

			String name = getName(o);
			String id = getIdOrDefault(o);

			Node r = new Node(name, id, o, highlight);

			if ((trace || !isTraceOnly(o)) && hasFeedbackLoop(o)) {
				FeedbackLoop loop = (FeedbackLoop) o;

				Object target = loop.delegateInput();
				if (target != null && target != loop && !virtualRef(target, r)) {
					Node input = expandReactiveSate(target);
					addEdge(r.createEdgeTo(input, Edge.Type.feedbackLoop));
					addDownstream(input, null);
				}

				target = loop.delegateOutput();

				if (target != null && target != loop && !virtualRef(target, r)) {
					Node output = expandReactiveSate(target);
					addEdge(output.createEdgeTo(r, Edge.Type.feedbackLoop));
					addUpstream(output, null);
				}
			}

			return r;
		}

		private void addEdge(Edge edge) {
			edges.put(edge.getId(), edge);
		}

		private boolean virtualRef(Object o, Node ancestor) {
			if (o != null && ancestor != null && String.class.isAssignableFrom(o.getClass())) {
				Node virtualNode = new Node(o.toString(), o.toString(), null, false);
				Edge edge = ancestor.createEdgeTo(o.toString(), Edge.Type.reference);
				virtualNode.addEdgeRef(edge);
				nodes.put(virtualNode.id, virtualNode);
				addEdge(edge);
				return true;
			}
			return false;
		}

		/**
		 *
		 * @param timestamp
		 * @return
		 */
		public String toString(boolean timestamp) {
			return "{" +
					" \"edges\" : " + edges.values() +
					(trace ? ", \"trace\" : true" : "") +
					", \"nodes\" : " + nodes.values() +
					(subscan ? ", \"full\" : false" : "") +
					(timestamp ? ", \"timestamp\" : " + System.currentTimeMillis() : "") +
					'}';
		}

		@Override
		public String toString() {
			return toString(true);
		}
	}

	/**
	 *
	 */
	public static class Node implements Comparable<Node> {

		transient private final Object  object;
		transient private       Edge[]  connectionsRef;
		private final           String  id;
		private final           String  name;
		private final           String group;
		private final           boolean unique;
		private final           boolean factory;
		private final           boolean inner;
		private final           boolean highlight;

		protected Node(String name, String id, Object o, boolean highlight) {
			this.highlight = highlight;
			this.object = o;
			this.id = id;
			this.name = name;
			this.factory = ReactiveStateUtils.isFactory(o);
			this.inner = isContained(o);
			this.group = ReactiveStateUtils.getGroup(o);
			this.unique = isUnique(o);
		}

		private void addEdgeRef(Edge edge) {
			if (connectionsRef == null) {
				connectionsRef = new Edge[1];
				connectionsRef[0] = edge;
				return;
			}
			int n = connectionsRef.length;
			Edge[] b = new Edge[n + 1];
			System.arraycopy(connectionsRef, 0, b, 0, n);
			b[n] = edge;
			connectionsRef = b;
		}

		public final Object value() {
			return object;
		}

		public final String getId() {
			return id;
		}

		public final String getName() {
			return name;
		}

		public final boolean isHighlight() {
			return highlight;
		}

		public final String getGroup() {
			return group;
		}

		public final boolean isFactory() {
			return factory;
		}

		public final boolean isInner() {
			return inner;
		}

		public final boolean isReference() {
			return object == null;
		}

		public final boolean isDefinedId() {
			return unique;
		}

		public final long getCapacity() {
			return ReactiveStateUtils.getCapacity(object);
		}

		public final long getBuffered() {
			return ReactiveStateUtils.getBuffered(object);
		}

		public final long getUpstreamLimit() {
			return ReactiveStateUtils.getUpstreamLimit(object);
		}

		public final long getPeriod() {
			return ReactiveStateUtils.getTimedPeriod(object);
		}

		public final Throwable getFailedState() {
			return ReactiveStateUtils.getFailedState(object);
		}

		public final long getExpectedUpstream() {
			return ReactiveStateUtils.getExpectedUpstream(object);
		}

		public final long getRequestedDownstream() {
			return ReactiveStateUtils.getRequestedDownstream(object);
		}

		public final Boolean isActive() {
			if (!hasSubscription(object)) {
				return null;
			}
			return ((ActiveUpstream) object).isStarted();
		}

		public final Boolean isTerminated() {
			if (!hasSubscription(object)) {
				return null;
			}
			return ((ActiveUpstream) object).isTerminated();
		}

		public final Boolean isCancelled() {
			if (!isCancellable(object)) {
				return null;
			}
			return ((ActiveDownstream) object).isCancelled();
		}

		protected final Edge createEdgeTo(Node to) {
			return createEdgeTo(to.id, null);
		}

		protected final Edge createEdgeTo(Node to, Edge.Type type) {
			return createEdgeTo(to.id, type);
		}

		protected final Edge createEdgeTo(String to, Edge.Type type) {
			Edge edge = new Edge(id, to, type);
			addEdgeRef(edge);
			return edge;
		}

		@Override
		public final boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			Node node = (Node) o;

			return id.equals(node.id);
		}

		@Override
		public final int hashCode() {
			return id.hashCode();
		}

		@Override
		public String toString() {
			return toPrettyString(-1);
		}

		public String toPrettyString() {
			return toPrettyString(1);
		}

		public String toPrettyString(int indent) {
			int i = indent;

			StringBuffer res = new StringBuffer();

			indent("{", res, indent != -1 ? 0 : -1, false);

			indent(property("id", getId()), res, i, true);
			if (isDefinedId()) {
				indent(property("definedId", "true"), res, i, true);
			}
			indent(property("name", getName()), res, i, true);
			if (isInner()) {
				indent(property("inner", "true"), res, i, true);
			}
			if (isReference()) {
				indent(property("reference", "true"), res, i, true);
			}
			else {
				if (getFailedState() != null) {
					indent(property("failed", getFailedState().getMessage()), res, i, true);
				}

				indent(property("period", getPeriod()), res, i, true);
				indent(property("capacity", getCapacity()), res, i, true);
				indent(property("group", getGroup()), res, i, true);
				//indent(property("type", object.getClass().getName()), res, i, true);
				indent(property("buffered", getBuffered()), res, i, true);

				if (isFactory()) {
					indent(property("factory", "true"), res, i, true);
				}

				if (isHighlight()) {
					indent(property("highlight", "true"), res, i, true);
				}

				indent(property("upstreamLimit", getUpstreamLimit()), res, i, true);
				indent(property("expectedUpstream", getExpectedUpstream()), res, i, true);
				indent(property("requestedDownstream", getRequestedDownstream()), res, i, true);
				indent(property("active", isActive()), res, i, true);
				indent(property("terminated", isTerminated()), res, i, true);
				indent(property("cancelled", isCancelled()), res, i, false);
			}

			indent("}", res, indent != -1 ? 0 : -1, false);

			return res.toString();
		}

		@Override
		public int compareTo(Node o) {
			return name.compareTo(o.name);
		}
	}

	/**
	 *
	 */
	public static class Edge {

		public enum Type {feedbackLoop, inner, reference}

		private final String from;
		private final String to;
		private final Type   type;

		protected Edge(String from, String to, Type type) {
			this.from = from;
			this.to = to;
			this.type = type;
		}

		public final String getFrom() {
			return from;
		}

		public final String getTo() {
			return to;
		}

		public final Type getType() {
			return type;
		}

		public final String getId() {
			return from + "_" + to;
		}

		@Override
		public final boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			Edge edge = (Edge) o;

			if (!from.equals(edge.from)) {
				return false;
			}
			return to.equals(edge.to);

		}

		@Override
		public final int hashCode() {
			int result = from.hashCode();
			result = 31 * result + to.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "{ " + property("id", getId()) +
					(type != null ? ", " + property("type", type.name()) : "") +
					", " + property("from", from) +
					", " + property("to", to) + " }";
		}
	}

	/**
	 *
	 * @param symbol
	 * @param res
	 * @param indent
	 * @param comma
	 */
	public static void indent(String symbol, StringBuffer res, int indent, boolean comma) {
		if (symbol.isEmpty()) {
			return;
		}
		for (int i = 0; i < indent; i++) {
			res.append("\t");
		}
		res.append(symbol);
		if (comma) {
			res.append(", ");
		}
		if (indent > -1) {
			res.append("\n");
		}
	}

	/**
	 *
	 * @param name
	 * @param value
	 * @return
	 */
	public static String property(String name, Object value) {
		if (value == null || value.equals(-1) || value.equals(-1L)) {
			return "";
		}

		if (Number.class.isAssignableFrom(value.getClass())) {
			if (Long.MAX_VALUE == ((Number) value).longValue()) {
				return "\"" + name + "\" : \"unbounded\"";
			}
			return "\"" + name + "\" : " + value.toString();
		}

		if (Boolean.class.isAssignableFrom(value.getClass())) {
			return "\"" + name + "\" : " + value.toString();
		}

		return "\"" + name + "\" : " +
				(String.class.isAssignableFrom(value.getClass()) ? "\"" + value.toString().replaceAll("\"", "\\\"")
						+ "\"" : value);
	}
}
