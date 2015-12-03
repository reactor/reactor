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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class ReactiveStateUtils {

	/**
	 * Create a "Nodes" and "Links" complete representation of a given component if available
	 * @param o
	 * @return a Graph
	 */
	public static Graph scan(Object o){
		if(o == null){
			return null;
		}

		Graph graph = new Graph();
		Node origin = new Node(o);
		graph.addUpstream(origin);
		graph.addDownstream(origin);

		return graph;
	}

	/**
	 * Create a "Nodes" and "Links" downstream representation of a given component if available
	 * @param o
	 * @return a Graph
	 */
	public static Graph subscan(Object o){
		if(o == null){
			return null;
		}

		Graph graph = new Graph(true);
		Node root = new Node(o);
		graph.addDownstream(root);

		return graph;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasUpstream(Object o) {
		return o != null && ReactiveState.Upstream.class.isAssignableFrom(o.getClass()) && ((ReactiveState.Upstream) o).upstream() != null;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasUpstreams(Object o){
		return o != null && ReactiveState.LinkedUpstreams.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasDownstream(Object o) {
		return o != null && ReactiveState.Downstream.class.isAssignableFrom(o.getClass()) && ((ReactiveState.Downstream) o).downstream() != null;
	}

	/**
	 *
	 * @param o
	 * @return
	 */
	public static boolean hasDownstreams(Object o){
		return o != null && ReactiveState.LinkedDownstreams.class.isAssignableFrom(o.getClass());
	}

	/**
	 *
	 */
	public static final class Graph {

		private final Set<Node> nodes = new HashSet<>();
		private final Set<Edge> edges = new HashSet<>();

		private final boolean subscan;

		Graph(){
			this(false);
		}

		Graph(boolean subscan){
			this.subscan = subscan;
		}

		public Set<Node> getNodes() {
			return nodes;
		}

		public Set<Edge> getEdges() {
			return edges;
		}

		private void addUpstream(Node target){
			nodes.add(target);
			if(hasUpstream(target.object)){
				Node upstream =  new Node(((ReactiveState.Upstream)target.object).upstream());
				edges.add(new Edge(upstream.id, target.id));
				addUpstream(upstream);
			}
			if(hasUpstreams(target.object)){
				addUpstreams(target, ((ReactiveState.LinkedUpstreams)target.object).upstreams());
			}
		}

		private void addUpstreams(Node target, Iterator o){
			Node source;
			while(o.hasNext()){
				source = new Node(o.next());
				edges.add(new Edge(source.id, target.id));
				addUpstream(source);
			}
		}

		private void addDownstream(Node root){
			nodes.add(root);
			if(hasDownstream(root.object)){
				Node downstream = new Node(((ReactiveState.Downstream)root.object).downstream());
				edges.add(new Edge(root.id, downstream.id));
				addDownstream(downstream);
			}
			if(hasDownstreams(root.object)){
				addDownstreams(root, ((ReactiveState.LinkedDownstreams)root.object).downstreams());
			}
		}

		private void addDownstreams(Node source, Iterator o){
			Node target;
			while(o.hasNext()){
				target = new Node(o.next());
				edges.add(new Edge(source.id, target.id));
				addDownstream(target);
			}
		}

		@Override
		public String toString() {
			return "{" +
					" full : " + !subscan +
					", edges : " + edges +
					", nodes : " + nodes +
					'}';
		}
	}

	/**
	 *
	 */
	public static final class Edge {

		private final String from;
		private final String to;

		Edge(String from, String to){
			this.from = from;
			this.to = to;
		}

		public String getFrom() {
			return from;
		}

		public String getTo() {
			return to;
		}

		@Override
		public boolean equals(Object o) {
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
		public int hashCode() {
			int result = from.hashCode();
			result = 31 * result + to.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "{ from : \"" + from + "\", to : \""+to+"\" }";
		}
	}

	/**
	 *
	 */
	public static final class Node {
		private final Object object;
		private final String id;
		private final String name;

		Node(Object o){
			this.object = o;
			this.name = ReactiveState.Named.class.isAssignableFrom(o.getClass()) ?
					(((ReactiveState.Named)o).getName()) :
					(o.getClass().getSimpleName());
			this.id = name+":"+o.hashCode();
		}

		public Object value() {
			return object;
		}

		public String getId() {
			return id;
		}

		public String getLabel() {
			return name;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			Node node = (Node) o;

			return object.equals(node.object);
		}

		@Override
		public int hashCode() {
			return object.hashCode();
		}

		@Override
		public String toString() {
			return "{ id : \"" + id +"\", label : \"" + name + "\" }";
		}
	}
}
