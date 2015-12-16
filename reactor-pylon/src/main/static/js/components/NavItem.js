'use strict';

import React         from 'react';
import {Link}        from 'react-router';

class NavItem extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        var classNameIcon = "fa fa-" + this.props.icon;
        return (
            <li>
                <Link activeClassName="active" to={this.props.to}>
                    <span className={classNameIcon}></span>
                    {this.props.label}
                </Link>
            </li>
        );
    }

}

export default NavItem;