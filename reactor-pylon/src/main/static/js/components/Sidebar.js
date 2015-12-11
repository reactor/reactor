'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import NavItem       from './NavItem';
import Status       from './Status';

class Sidebar extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <div id="sidebar">
                <Status {...this.props} />
                <ul>
                    <NavItem to="/pylon/dashboard" name="dashboard" label="Dashboard" icon="tachometer" />
                    <NavItem to="/pylon/metrics" name="metrics" label="Metrics" icon="area-chart" />
                    <NavItem to="/pylon/streams" name="streams" label="Streams" icon="sliders" />
                    <NavItem to="/pylon/hosts" name="hosts" label="Hosts" icon="server" />
                    <NavItem to="/pylon/system" name="system" label="System" icon="cogs" />
                    <NavItem to="/pylon/logs" name="logs" label="Logs Tail" icon="newspaper-o" />
                    <NavItem to="/pylon/studio" name="studio" label="Studio" icon="camera-retro" />
                </ul>
                <span id="selection"></span>
            </div>
        );
    }

}

export default Sidebar;