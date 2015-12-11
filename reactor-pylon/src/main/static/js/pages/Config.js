'use strict';

import React         from 'react';
import API           from '../services/NexusService';
import DocumentTitle from 'react-document-title';
import Routes from '../Routes';

const propTypes = {
};

class Config extends React.Component {

    constructor(props) {
        super(props);
    }

    componentDidMount() {
        this.refs.url.value = Config.formatUrl(API.defaultOrLastTarget());
    }

    onCheck(e){
        console.log(e);
    }

    onSubmit(e) {
        e.preventDefault();
        var url = Config.formatUrl(this.refs.url.value);
        this.props.connect(url);
    }

    static formatUrl(uri){
        if(uri.indexOf('://') == -1){
            return 'ws://'+uri;
        }
        return uri;
    }

    render() {
        return (
            <div id="config">
                <div className="header">
                    <h1 id="logo"><a>Reactor Pylon</a></h1>
                    <p className="description">Connect to the Nexus API to start monitoring a Reactor System.</p>
                    <form onSubmit={this.onSubmit.bind(this)}>
                        <p>
                            <input ref="url" placeholder="API URL to monitor" className="form-control" type="text" />
                        </p>
                        <p className="checkbox">
                            <label><input onChange={this.onCheck.bind(this)} ref="opt1" checked type="checkbox" /> Graph Stream</label>
                            <label><input onChange={this.onCheck.bind(this)} ref="opt1" checked type="checkbox" /> System Stats Stream</label>
                            <label><input onChange={this.onCheck.bind(this)} ref="opt1" checked type="checkbox" /> Log Stream</label>
                            <label><input onChange={this.onCheck.bind(this)} ref="opt1" checked type="checkbox" /> Metrics Stream</label>
                        </p>
                        <p className="action">
                            <button className="btn btn-primary btn-block" type="submit">Run</button>
                        </p>
                    </form>
                </div>
            </div>
        );
    }

}

Config.propTypes = propTypes;

export default Config;