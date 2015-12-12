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
import DocumentTitle from 'react-document-title';
import vis           from 'vis';
import Box           from '../components/Box';
import StreamGraph           from '../components/StreamGraph';
import JSON           from 'JSON2';
import Rx            from 'rx-lite';
import ReactDOM      from 'react-dom';
import stripComments      from 'strip-json-comments';

const propTypes = {
    network: React.PropTypes.object, nodes: React.PropTypes.object, edges: React.PropTypes.object
};

class Studio extends React.Component {

    constructor(props) {
        super(props);

        this.timeline = null;
        this.records = new vis.DataSet();
        this.formEvents = new Rx.Subject();
        this.graphControlEvents = new Rx.Subject();

        var thiz = this;
        this.formEventsStream = this.formEvents
            .flatMap(d => {
                try{
                    if(d.parsed !== undefined){
                        return Rx.Observable.just(d);
                    }

                    var parsed = JSON.parse(stripComments(d));


                    if(parsed.constructor === Array){
                        console.log("Graph collections", parsed);
                        return Rx.Observable.from(parsed).doOnNext(thiz.addTimelineItem.bind(thiz));
                    }
                    else{
                        return Rx.Observable.just(parsed).doOnNext(thiz.addTimelineItem.bind(thiz));
                    }
                }
                catch(e){
                    console.log("Fallback to line by line parsing : ", e);
                    Rx.Observable
                        .from(d.split("\n"))
                        .filter(d => d.trim())
                        .map(d => JSON.parse(stripComments(d))).subscribe(thiz.addTimelineItem.bind(thiz));

                    return Rx.Observable.empty();
                }

            });
    }

    onSubmit(e) {
        e.preventDefault();
        this.formEvents.onNext(this.refs.replay.value);
    }

    addTimelineItem(item){
        item.start = new Date(item.timestamp);
        item.content = new Date(item.timestamp);
        item.parsed = true;
        this.records.add(item);

        var start = new Date(item.timestamp - 30 * 1000 * 20);
        var end   = new Date(item.timestamp + 60 * 1000 * 20);
        this.timeline.setWindow(start, end);
        this.timeline.on("select", this.onSelectTimelineItem.bind(this));
    }

    onSelectTimelineItem(item){
        if(item.items.length == 0){
            return;
        }

        this.graphControlEvents.onNext({type: 'clear'});

        for(var id in item.items) {
            var node = this.records.get(item.items[id]);
            if (node == null) {
                continue;
            }

            this.formEvents.onNext(node);
        }
        //ReactDOM.render(<span>HELLO {item.content}</span>, document.getElementById("timeline-tools"));
    }

    requestFullscreen(e){
        this.graphControlEvents.onNext({type: 'fullscreen'});
    }

    componentDidMount(){
        var container = document.getElementById("timeline-box");
        var options = {
            type: 'point',
            throttleRedraw: 15,
            align: "left",
            multiselect: true,
            showCurrentTime: true
        };
        this.timeline = new vis.Timeline(container, null, options);
        this.timeline.setItems(this.records);

    }

    render() {
        return (
            <DocumentTitle title="Reactor Console • Studio">
                <section className="studio">
                    <div className="section-heading">
                        Studio
                    </div>
                    <div className="section-content">
                        <Box cols="1" heading="Observing Station" onClick={this.requestFullscreen.bind(this)}>
                            <div id="observing">
                                <StreamGraph controlBus={this.graphControlEvents} graphOptions={{interaction: {
                                    dragNodes: true, zoomView: true, hover: true, }}} streams={this.formEventsStream}/>
                            </div>
                        </Box>
                        <Box heading="Editor">
                            <div className="editor">
                                <form onSubmit={this.onSubmit.bind(this)}>
                                    <p>
                                        <textarea ref="replay"></textarea>
                                    </p>
                                    <p className="action">
                                        <button className="btn btn-primary btn-block" type="submit">Run</button>
                                    </p>
                                </form>
                            </div>
                        </Box>

                        <Box heading="Timeline">
                            <div id="timeline-box"></div>
                            <div id="timeline-tools"></div>
                        </Box>
                    </div>
                </section>
            </DocumentTitle>
        );
    }

}

Map.propTypes = propTypes;

export default Studio;