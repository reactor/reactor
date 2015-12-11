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

        this.formEvents = new Rx.Subject();
        this.fullscreenEvents = new Rx.Subject();
        this.formEventsStream = this.formEvents
            .flatMap(d => {
                try{
                    var parsed = JSON.parse(stripComments(d));
                    if(parsed.constructor === Array){
                        console.log("Graph collections", parsed);
                        return Rx.Observable.from(parsed)
                    }
                    else{
                        return Rx.Observable.just(parsed);
                    }
                }
                catch(e){
                    console.log("Fallback to line by line parsing : ", e);
                    return Rx.Observable
                        .from(d.split("\n"))
                        .filter(d => d.trim())
                        .map(d => JSON.parse(stripComments(d)));
                }

            });
    }

    onSubmit(e) {
        e.preventDefault();
        this.formEvents.onNext(this.refs.replay.value);
    }

    requestFullscreen(e){
        this.fullscreenEvents.onNext(e);
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
                                <StreamGraph onFullscreen={this.fullscreenEvents} graphOptions={{interaction: {
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
                            <div id="timeline"></div>
                        </Box>
                    </div>
                </section>
            </DocumentTitle>
        );
    }

}

Map.propTypes = propTypes;

export default Studio;