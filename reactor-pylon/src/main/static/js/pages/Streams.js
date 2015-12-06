'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';
import vis           from 'vis';
import ReactDOM        from 'react-dom';
import Rx              from 'rx';

const propTypes = {
    network: React.PropTypes.object,
    nodes: React.PropTypes.object,
    edges: React.PropTypes.object
};


var percentColors = [
    { pct: 0.0, color: { r: 0xff, g: 0x00, b: 0 } },
    { pct: 0.5, color: { r: 0xff, g: 0xff, b: 0 } },
    { pct: 1.0, color: { r: 0x00, g: 0xff, b: 0 } }
];

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

class Streams extends React.Component {

    constructor(props) {
        super(props);
        this.network = null;

        this.nodes = new vis.DataSet();
        this.edges = new vis.DataSet();
    }

    destroy() {
        this.resetAllNodes();
        if (this.network !== null) {
            this.network.destroy();
            this.network = null;
        }
    }

    resetAllNodes() {
        this.nodes.clear();
        this.edges.clear();
    }

    resetAllNodesStabilize() {
        this.resetAllNodes();
        this.network.stabilize();
    }

    loadJSON(path, success, error) {
        var xhr = new XMLHttpRequest();
        xhr.onreadystatechange = function () {
            if (xhr.readyState === 4) {
                if (xhr.status === 200) {
                    success(JSON.parse(xhr.responseText));
                }
                else {
                    error(xhr);
                }
            }
        };
        xhr.open('GET', path, true);
        xhr.send();
    }

    draw() {
        this.destroy();
        var container = document.getElementById('mynetwork');
        var options = {
            layout: {
                randomSeed: 100
            },
            interaction: {
                dragNodes: false, zoomView: false, hover: true, tooltipDelay: 0
            },
            clickToUse: true,
            nodes: {
                labelHighlightBold: false,
                color: {
                    highlight: {
                        border: '#6db33f', background: '#34302d'
                    }, border: '#6db33f', background: '#6db33f'
                }, shape: 'diamond', font: {
                    size: 18, face: 'Montserrat', color: '#34302d'
                }, borderWidth: 2, scaling: {
                    min: 20, label: {
                        min: 16, max: 24
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
        };
        this.network = new vis.Network(container, {}, options);

        //var task =  function(){ test();setTimeout(task, 200); };
        //
        //setTimeout(task, 200);

        // add event listeners
        this.network.on('selectNode', function (params) {
            document.getElementById('selection').innerHTML = 'Selection: ' + params.nodes;
        });
        this.network.on('hoverNode', function (params) {
            ReactDOM.render(<ul>
                <li>{nodes.get(params.node).name}</li>
                <li>Capacity : {nodes.get(params.node).capacity}</li>
                <li>Cancelled : {nodes.get(params.node).cancelled+""}</li>
                <li>Started : {nodes.get(params.node).active+""}</li>
                <li>Terminated : {nodes.get(params.node).terminated+''}</li>
                <li>Buffered : {nodes.get(params.node).buffered+''}</li>
            </ul>, document.getElementById('selection'));
        });

        var nodes = this.nodes;
        var edges = this.edges;
        var network = this.network;
        // randomly create some nodes and edges
        this.loadJSON("http://localhost:12012/nexus/stream", function (json) {
            //var data = getScaleFreeNetwork(nodeCount)

            var highlights = [];
            var n, e;
            for (var node in json.nodes) {
                n = json.nodes[node];
                if (n.highlight) {
                    highlights.push(n.id);
                }
                console.log(n);
                if(!n.active || n.cancelled || n.terminated){
                    n.shape = "dot"
                    n.color = {
                        border: n.terminated ? "#6db33f" : "gray",
                        background: "#f1f1f1"
                    };
                    if(!n.active && !n.cancelled && !n.terminated) {
                        n.shapeProperties = {borderDashes: [10, 10]};
                    }
                    n.value = 0;
                }
                else {
                  if (n.capacity != -1) {
                      n.shape = "dot"
                      if (n.capacity == 9223372036854775807) {
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
            }
            for (var edge in json.edges) {
                e = json.edges[edge];
                e.arrows = {to: true};
                if(e.discrete){
                    e.dashes = true;
                }
            }
            nodes.add(json.nodes);
            edges.add(json.edges);
            network.setData({nodes: nodes, edges: edges});
            network.selectNodes(highlights);

            //font: {size:15, color:'red', face:'courier', strokeWidth:3, strokeColor:'#ffffff'}
            // create a network

        }, function (error) {
            console.log(error);
        });
        return false;
    }

    componentDidMount() {
        this.draw();
    }

    componentDidUpdate() {
        this.draw();
    }

    render() {
        return (
            <DocumentTitle title="Streams">
                <section className="streams">
                    <h2>Stream Monitor <a href="#" onClick={this.draw}>Reset</a></h2>

                    <div id="mynetwork"></div>

                    <h3 id="selection"></h3>
                </section>
            </DocumentTitle>
        );
    }

}

Streams.propTypes = propTypes;
Streams.graphUtils = graphUtils;

export default Streams;