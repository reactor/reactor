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
import ReactDOM      from 'react-dom';

const propTypes = {
    controlBus: React.PropTypes.object, chartOptions: React.PropTypes.object
};

class CapacityDoughnut extends React.Component {

    constructor(props) {
        super(props);
        this.disposable = null;

        var thiz = this;
        this.state = {
            last: thiz.props.last
        };

        if (this.state.last.capacity != -1 && this.state.last.capacity != 'unbounded') {
            this.state.last.free = this.state.last.capacity - this.state.last.buffered;
        }

        if (this.props.controlBus !== undefined) {
            this.props.controlBus.subscribe(this.controlBusHandler.bind(this));
        }
    }

    destroy() {
        if (this.disposable != null) {
            this.disposable.dispose();
        }
    }

    controlBusHandler(event) {
    }

    draw(json) {
        this.state.last = json;
        if (json.capacity != -1 && json.capacity != 'unbounded') {
            json.free = json.capacity - json.buffered;
        }

        this.setState({last: json});
    }

    componentWillUnmount() {
        this.destroy();
    }

    componentDidMount() {
        if (this.props.updates !== undefined) {
            this.disposable = this.props.updates
                .subscribe(this.draw.bind(this), error => console.log(error), () => console.log("terminated"));
        }
    }

    render() {
        var value = ((this.state.last.buffered / (this.state.last.buffered + this.state.last.free)) * 100)
            .toFixed(1);
        return (
            <div className="box buffer" style={{height:'130px', width:'130px'}}>
                <div className="meter">
                    <span style={{width: value+'%'}}></span>
                </div>
                <div>{value}%</div>
                <div>({this.state.last.buffered}/{this.state.last.capacity})</div>
                <div className="name">{this.state.last.name}</div>
            </div>
        );
    }

}

CapacityDoughnut.propTypes = propTypes;

export default CapacityDoughnut;