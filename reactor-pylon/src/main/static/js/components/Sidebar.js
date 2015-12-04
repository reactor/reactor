'use strict';

import React         from 'react';
import {Link}        from 'react-router';
import NavItem       from './NavItem';

class Sidebar extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        return (
            <div id="sidebar">
                <h1 id="logo"><a href="/"><strong>Reactor Console</strong></a></h1>
                <ul>
                    <NavItem to="dashboard" name="dashboard" label="Dashboard" icon="tachometer" />
                    <NavItem to="map" name="map" label="Map" icon="share-alt" />
                    <NavItem to="streams" name="streams" label="Streams" icon="sliders" />
                    <NavItem to="hosts" name="hosts" label="Hosts" icon="server" />
                    <NavItem to="system" name="system" label="System" icon="cogs" />
                </ul>
            </div>
        );
    }

}

export default Sidebar;