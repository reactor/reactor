'use strict';

import React         from 'react';
import Box           from '../components/Box';
import ThreadTimeline from '../components/ThreadTimeline';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';
import vis           from 'vis';
import Rx            from 'rx-lite';

const propTypes = {
    dataset: React.PropTypes.object, graph: React.PropTypes.object
};

var DELAY = 1000;
var strategy = "continuous";

class System extends React.Component {

    constructor(props) {
        super(props);
        this.graph = null;
        this.dataset = null;
    }

    /**
     * Add a new datapoint to the graph
     */
    addDataPoint(json) {
        // add a new data point to the dataset
        this.dataset.add({
            x: new Date(json.timestamp), y: json.jvmStats.freeMemory
        });

        // remove all data points which are no longer visible
        var range = this.graph.getWindow();
        var interval = range.end - range.start;
        var oldIds = this.dataset.getIds({
            filter: function (item) {
                return item.x < range.start - interval;
            }
        });
        this.dataset.remove(oldIds);
    }

    renderStep() {
        // move the window (you can think of different strategies).
        var now = vis.moment();
        var range = this.graph.getWindow();
        var interval = range.end - range.start;
        switch (strategy) {
            case 'continuous':
                // continuously move the window
                this.graph.setWindow(now - interval, now, {animation: false});
                requestAnimationFrame(this.renderStep.bind(this));
                break;

            case 'discrete':
                this.graph.setWindow(now - interval, now, {animation: false});
                setTimeout(this.renderStep.bind(this), DELAY);
                break;

            default: // 'static'
                // move the window 90% to the left when now is larger than the end of the window
                if (now > range.end) {
                    this.graph.setWindow(now - 0.1 * interval, now + 0.9 * interval);
                }
                setTimeout(this.renderStep.bind(this), DELAY);
                break;
        }
    }

    draw() {
        if (this.graph == null) {
            // create a dataSet with groups

            // create a graph2d with an (currently empty) dataset
            var container = document.getElementById('graphProcessor');
            this.dataset = new vis.DataSet();

            var options = {
                start: vis.moment().add(-30, 'seconds'), // changed so its faster
                showMajorLabels: false,
                end: vis.moment(), dataAxis: {
                    left: {
                        range : {
                            min: 0, max: 1024 * 10 * 10 * 10 * 10 * 32
                        }
                    }
                }, drawPoints: {
                    style: 'circle' // square, circle
                }, shaded: {
                    orientation: 'bottom' // top, bottom
                }
            };
            this.graph = new vis.Graph2d(container, this.dataset, options);

            this.renderStep();
        }
    }

    componentDidMount() {
        this.draw();
        var thiz = this;
        this.disposable = this.props.systemStream
            .subscribe( json => {
                thiz.addDataPoint(json);
            }, error =>{
                console.log("error:", error);
            }, () => {
                console.log("terminated");
            });
    }

    componentWillUnmount() {
        this.disposable.dispose();
    }

    render() {
        return (
            <DocumentTitle title="Reactor Console • System">
                <section className="system">
                    <div className="section-heading">
                        JVM Systems
                    </div>
                    <div className="section-content">

                        <Box heading="Threads">
                            <ThreadTimeline threadStream={this.props.systemStream
                                .flatMap(json => Rx.Observable.from(json.threads).doOnNext(t =>
                                    t.timestamp = json.timestamp
                                ))} />
                        </Box>

                        <Box heading="Free Memory">
                            <div id="graphProcessor"></div>
                        </Box>
                        <Box heading="Total Memory">
                            Un autre exemple au fromage.
                        </Box>
                    </div>
                </section>
            </DocumentTitle>
        );
    }
}

System.propTypes = propTypes;

export default System;