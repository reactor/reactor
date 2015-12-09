
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
        this.threadStates = new vis.DataSet();
        this.threads = new vis.DataSet();
        this.timeline = null;
    }

    draw(){
        if(this.timeline == null) {
            var options = {
                stack: false,
                throttleRedraw: 15,
                align: "left",
                showCurrentTime: true
            };
            var container = document.getElementById('thread-timeline');
            this.timeline = new vis.Timeline(container, null, options);
            this.timeline.setGroups(this.threads);
            this.timeline.setItems(this.threadStates);
        }


        // create a data set
        var start = new Date((new Date()).getTime() - 30 * 1000);
        var end   = new Date((new Date()).getTime() + 60 * 1000);

        // timeline.addCustomTime(new Date());

        // set a custom range from -1 minute to +3 minutes current time
        this.timeline.setWindow(start, end, {animation: false});
    }

    static mapStateColor(state){
        if (state == 'WAITING') {
            return "waitingThread";
        }
        else if (state == 'TIMED_WAITING') {
            return "timedWaitingThread";
        }
        else if (state == 'RUNNABLE') {
            return "runnableThread";
        }
        else if (state == 'BLOCKED') {
            return"blockedThread";
        }
        else if (state == 'TERMINATED') {
            return "terminatedThread";
        }
        else if (state == 'NEW') {
            return"newThread";
        }
    }

    updateThread(nextThreadState) {
        var oldState = this.threads.get(nextThreadState.id);
        var lastId = null;
        var timestamp = new Date();
        if (oldState == null) {
            nextThreadState.content = nextThreadState.name;

            nextThreadState.lastStateId = nextThreadState.id + ":"+ timestamp.getTime();

            lastId = nextThreadState.lastStateId;
            this.threadStates.add({
                group : nextThreadState.id,
                id : nextThreadState.lastStateId,
                start : timestamp,
                end: new Date(timestamp.getTime() + 1000), //+1 sec
                className: ThreadTimeline.mapStateColor(nextThreadState.state)
            });
            this.threads.add(nextThreadState);

        }
        else if (nextThreadState.state != oldState.state) {
                var lastThreadItem = this.threadStates.get(oldState.lastStateId);
                nextThreadState.lastStateId = oldState.id+":"+timestamp.getTime();
                lastId = nextThreadState.lastStateId;

                this.threadStates.add({
                    group : nextThreadState.id,
                    id : nextThreadState.lastStateId,
                    start : new Date(lastThreadItem.end.getTime()),
                    end: new Date(lastThreadItem.end.getTime() + 1000), //+1 sec
                    className: ThreadTimeline.mapStateColor(nextThreadState.state)
                });
                this.threads.update(nextThreadState);
        }
        else{
                if(nextThreadState.state != 'TERMINATED') {
                    var lastThreadItem = this.threadStates.get(oldState.lastStateId);
                    lastId = oldState.lastStateId;
                    lastThreadItem.end = new Date(lastThreadItem.end.getTime() + 1000); //+1 sec
                    this.threadStates.update(lastThreadItem);
                }
                this.threads.update(nextThreadState);
        }

        if (lastId != null && this.timeline != null) {
            this.timeline.focus([lastId], {animation: {duration: 300, easingFunction: 'linear'}}); // ms
        }
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