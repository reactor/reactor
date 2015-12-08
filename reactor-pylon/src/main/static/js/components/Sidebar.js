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
                    <NavItem to="/pylon/map" name="map" label="Map" icon="share-alt" />
                    <NavItem to="/pylon/streams" name="streams" label="Streams" icon="sliders" />
                    <NavItem to="/pylon/hosts" name="hosts" label="Hosts" icon="server" />
                    <NavItem to="/pylon/system" name="system" label="System" icon="cogs" />
                </ul>
                <span id="selection"></span>
            </div>
        );
    }

}

export default Sidebar;