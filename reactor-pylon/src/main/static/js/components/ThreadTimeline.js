
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

class ThreadTimeline extends React.Component {

    constructor(props) {
        super(props);
        this.threads = new vis.DataSet();
    }

    draw(){

        // create a data set

        var start = new Date((new Date()).getTime() - 1 * 60 * 1000);
        var end   = new Date((new Date()).getTime() + 3 * 60 * 1000);
        var container = document.getElementById('thread-timeline');

        var options = {
            showCurrentTime: true
        };
        var timeline = new vis.Timeline(container, this.threads, options);

        // timeline.addCustomTime(new Date());

        // set a custom range from -1 minute to +3 minutes current time
        timeline.setWindow(start, end, {animation: false});
    }

    updateThread(nextThreadState){
        var oldState = this.threads.get(nextThreadState.id);
        if(oldState == null) {
            nextThreadState.content = nextThreadState.name;
            nextThreadState.start = new Date();
            nextThreadState.end = new Date((new Date()).getTime() + 1000); //+1 sec
        }
        else {
            nextThreadState.end = new Date(oldState.end.getTime() + 1000); //+1 sec
            if (nextThreadState.state == oldState.state) {
                //ignore
            }
            else if (nextThreadState.state == 'WAITING') {
                //nextThreadState.className =
            }
            else if (nextThreadState.state == 'TIMED_WAITING') {

            }
            else if (nextThreadState.state == 'RUNNABLE') {

            }
            else if (nextThreadState.state == 'BLOCKED') {

            }
            else if (nextThreadState.state == 'TERMINATED') {

            }
            else if (nextThreadState.state == 'NEW') {

            }
        }

        this.threads.update(nextThreadState);
    }

    componentDidMount() {
        this.draw();
        var thiz = this;
        this.disposable = this.props.threadStream
            .subscribe(
                thiz.updateThread.bind(this),
                e => console.log(e),
                () => console.log("Thread timeline terminated")
            );

    }

    render() {
        return (
            <div id="thread-timeline">
            </div>
        );
    }

}


export default ThreadTimeline;