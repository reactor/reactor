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
import Rx            from 'rx-lite';
import Box            from '../components/Box';

const propTypes = {
    network: React.PropTypes.object, nodes: React.PropTypes.object, edges: React.PropTypes.object
};

class Logs extends React.Component {

    constructor(props) {
        super(props);

        this.state = {
            logs : [],
            id: 1
        };
        var thiz = this;
        this.disposable = this.props.logStream.subscribe(json => {
            json.id = thiz.state.id++;
            if(thiz.state.logs.length > 200){
                thiz.state.logs.shift();
            }
            thiz.state.logs.push(json);
            thiz.setState({logs: thiz.state.logs});
        });
    }

    render() {
        return (
            <DocumentTitle title="Reactor Console • Logs">
                <section className="logs">
                    <div className="section-heading">
                        Logs
                    </div>
                    <div className="section-content">
                        <Box cols="1" heading="Tail">
                            {this.state.logs.map( json => {
                                return <div key={json.id}><span>{new Date(json.timestamp).toTimeString()}</span><span>{json.message}</span></div>
                            })}
                        </Box>
                    </div>
                </section>
            </DocumentTitle>
        );
    }

}

Map.propTypes = propTypes;

export default Logs;