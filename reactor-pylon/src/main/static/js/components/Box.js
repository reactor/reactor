'use strict';

import React         from 'react';
import {Link}        from 'react-router';

class Box extends React.Component {

    constructor(props) {
        super(props);
    }

    render() {
        var b = null;
        if (this.props.onClick !== undefined){
            b = <span className="fa fa-tv" onClick={this.props.onClick} />
        }

        return (
            <div className={(this.props.cols !== undefined ? 'box-'+this.props.cols : '') + ' box'}>
                <h2 className="box-title">{this.props.heading} {b}</h2>
                <div className="box-content">{this.props.children}</div>
            </div>
        );
    }

}


export default Box;