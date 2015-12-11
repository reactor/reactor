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

'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import vis           from 'vis';
import ReactDOM      from 'react-dom';

const propTypes = {
    network: React.PropTypes.object, nodes: React.PropTypes.object, edges: React.PropTypes.object
};

var percentColors = [{pct: 0.0, color: {r: 0xff, g: 0x00, b: 0}}, {pct: 0.5, color: {r: 0xff, g: 0xff, b: 0}},
    {pct: 1.0, color: {r: 0x00, g: 0xff, b: 0}}];

const graphUtils = {
    getColorForPercentage(pct) {
        for (var i = 1; i < percentColors.length - 1; i++) {
            if (pct < percentColors[i].pct) {
                break;
            }
        }
        var lower = percentColors[i - 1];
        var upper = percentColors[i];
        var range = upper.pct - lower.pct;
        var rangePct = (pct - lower.pct) / range;
        var pctLower = 1 - rangePct;
        var pctUpper = rangePct;
        var color = {
            r: Math.floor(lower.color.r * pctLower + upper.color.r * pctUpper),
            g: Math.floor(lower.color.g * pctLower + upper.color.g * pctUpper),
            b: Math.floor(lower.color.b * pctLower + upper.color.b * pctUpper)
        };
        return 'rgb(' + [color.r, color.g, color.b].join(',') + ')';
    }
};

class StreamGraph extends React.Component {

    constructor(props) {
        super(props);
        this.disposable = null;

        this.network = null;
        this.nodes = new vis.DataSet();
        this.edges = new vis.DataSet();
        this.graphOptions = this.props.graphOptions;

        if(this.props.resetNodes !== undefined){
            this.props.resetNodes.subscribe(this.resetAllNodesStabilize.bind(this));
        }
        if(this.props.onFullscreen !== undefined){
            this.props.onFullscreen.subscribe(this.fullscreen.bind(this));
        }
    }

    destroy() {
        if (this.disposable != null) {
            this.disposable.dispose();
        }

        this.resetAllNodes();
        if (this.network != null) {
            this.network.destroy();
            this.network = null;
        }
    }

    resetAllNodes() {
        this.nodes.clear();
        this.edges.clear();
    }

    resetAllNodesStabilize() {
        if (this.network != null) {
            var nodes = this.nodes;
            var edges = this.edges;
            this.network.setData({nodes: nodes, edges: edges});
        }

    }

    fullscreen(e){
        if(this.fullscreenMode === undefined){
            this.fullscreenMode = true;
        }
        else if(this.fullscreenMode){
            this.fullscreenMode = false;
        }

        var container = document.getElementById('stream-graph');
        if(this.fullscreenMode) {
            var h = this.calcHeight();
            container.style.height = (h.y - 110) + "px";
        }
        else{
            container.style.height = "260px";
        }
    }

    draw(json) {
        //this.destroy();
        var container = document.getElementById('stream-graph');

        // height
        if(this.props.fullscreen) {
            var h = this.calcHeight();
            container.style.height = (h.y - 110) + "px";
        }

        var options = Object.assign({
            layout: {
                randomSeed: 100
            }, interaction: {
                dragNodes: false, zoomView: false, hover: true, tooltipDelay: 0
            }, //clickToUse: true,
            nodes: {
                labelHighlightBold: false, color: {
                    highlight: {
                        // border: '#6db33f', background: '#34302d'
                    }, border: '#6db33f', background: '#6db33f'
                }, shape: 'diamond', font: {
                    size: 18, face: 'Montserrat', color: '#34302d'
                }, borderWidth: 2, scaling: {
                    min: 20, label: {
                        min: 12, max: 18
                    }
                }
            }, edges: {
                smooth: true, //smooth: {
                //    type: 'dynamic'
                //},
                //dashes: true,
                width: 2
            }, "physics": {
                //"hierarchicalRepulsion": {
                //    "nodeDistance": 190
                //},
                //"minVelocity": 0.75,
                //"barnesHut": {
                //    "avoidOverlap": 2,
                //    springLength: 300
                //}
                //"solver": "hierarchicalRepulsion"
            }
        }, this.graphOptions);

        var nodes = this.nodes;
        var edges = this.edges;
        var network = this.network;
        var first = false;
        if (network == null) {
            network = new vis.Network(container, {}, options);
            this.network = network;
            first = true;
        }

        //var task =  function(){ test();setTimeout(task, 200); };
        //
        //setTimeout(task, 200);

        // add event listeners
        this.network.on('selectNode', (params) => {
            document.getElementById('selection').innerHTML = 'Selection: ' + params.nodes;
        });
        this.network.on('hoverNode', (params) => {
            ReactDOM.render(<pre className="select">
                {JSON.stringify(nodes.get(params.node), null, 2)}
            </pre>, document.getElementById('selection'));
        });

        // randomly create some nodes and edges

        var highlights = [];
        var n, e;
        for (var node in json.nodes) {
            n = json.nodes[node];
            if (n.highlight) {
                highlights.push(n.id);
            }
            if (!n.active || n.cancelled || n.terminated) {
                n.shape = "dot";
                n.color = {
                    border: n.terminated ? "#6db33f" : "gray", background: "#f1f1f1"
                };
                if (!n.active && !n.cancelled && !n.terminated) {
                    n.shapeProperties = {borderDashes: [10, 10]};
                }
                n.value = 0;
            }
            else {
                if (n.capacity != -1) {
                    n.shape = "dot";
                    if (n.capacity == "unbounded") {
                        n.color = {
                            border: "green", background: "#f1f1f1"
                        };
                        n.shapeProperties = {borderDashes: [10, 10]};
                    }
                    else {
                        n.value = n.capacity;
                        var backgroundColor = n.buffered != -1 ?
                            graphUtils.getColorForPercentage(1 - (n.buffered / n.capacity)) : "#6db33f";
                        n.color = {
                            border: "green", background: backgroundColor
                        };
                    }
                }
                else {
                    n.value = 0;
                }
            }
            n.label = n.name;

            if (n.definedId) {
                n.shape = "square";
            }
            else if(n.inner){
                n.shape = "cicle";
                n.label = n.buffered != -1 ? n.buffered : " ";
            }
        }
        for (var edge in json.edges) {
            e = json.edges[edge];
            e.arrows = {to: true};
            if (e.discrete) {
                e.dashes = true;
            }
        }
        nodes.update(json.nodes);
        edges.update(json.edges);

        if (first) {
            network.setData({nodes: nodes, edges: edges});
            network.selectNodes(highlights);
        }

        //font: {size:15, color:'red', face:'courier', strokeWidth:3, strokeColor:'#ffffff'}
        // create a network

        return false;
    }

    componentWillUnmount() {
        this.disposable.dispose();
        this.destroy();
    }

    componentDidMount() {
        this.disposable = this.props.streams
            .subscribe(this.draw.bind(this), error => console.log("error:", error), () => console.log("terminated"));
    }

    calcHeight() {
        var w = window, d = document, e = d.documentElement, g = d.getElementsByTagName('body')[0], x = w.innerWidth ||
            e.clientWidth || g.clientWidth, y = w.innerHeight || e.clientHeight || g.clientHeight;

        return {x: x, y: y};
    }

    render() {
        return (
            <div id="stream-graph">
            </div>
        );
    }

}

StreamGraph.propTypes = propTypes;
StreamGraph.graphUtils = graphUtils;


export default StreamGraph;