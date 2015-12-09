'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import DocumentTitle from 'react-document-title';

const propTypes = {
};

class Config extends React.Component {

    constructor(props) {
        super(props);
        this.onSubmit = this.onSubmit.bind(this)
    }

    componentDidMount() {
        this.refs.url.value = "http://localhost/API";
    }

    onSubmit(e) {
        e.preventDefault();
        console.log(this.refs.url.value);
        console.log(this.refs.opt1.checked);
    }

    render() {
        return (
            <div id="config">
                <div className="header">
                    <h1 id="logo"><a>Reactor console</a></h1>
                    <p className="description">Connect to the Nexus API to start monitoring a Reactive System.</p>
                    <form onSubmit={this.onSubmit}>
                        <p>
                            <input ref="url" placeholder="API URL to monitor" className="form-control" type="text" />
                        </p>
                        <p className="checkbox">
                            <label><input ref="opt1" checked type="checkbox" /> Graph Stream</label>
                            <label><input ref="opt1" checked type="checkbox" /> System Stats Stream</label>
                            <label><input ref="opt1" checked type="checkbox" /> Log Stream</label>
                            <label><input ref="opt1" checked type="checkbox" /> Metrics Stream</label>
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