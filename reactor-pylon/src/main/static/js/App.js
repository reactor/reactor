'use strict';

import React              from 'react';

import Sidebar            from './components/Sidebar';

const propTypes = {
  params: React.PropTypes.object,
  nexusFeed: React.PropTypes.object,
  query: React.PropTypes.object,
  children: React.PropTypes.oneOfType([
    React.PropTypes.array,
    React.PropTypes.object
  ])
};

class App extends React.Component {

  constructor(props) {
    super(props);
    this.state = {
      //nexusFeed: from,
    };
  }

  componentWillMount() {
  }

  componentDidMount() {
  }

  componentWillUnmount() {
  }

  renderChildren() {
    return React.cloneElement(this.props.children, {
      params: this.props.params,
      query: this.props.query,
      currentUser: this.state.currentUser
    });
  }

  render() {
    return (
      <div>

        <Sidebar />
        <div id='main'>
          {this.renderChildren()}
        </div>
      </div>
    );
  }

}

App.propTypes = propTypes;

export default App;