'use strict';

import React              from 'react';

import Sidebar            from './components/Sidebar';
import API                from './utils/APIUtils';
import Rx                 from 'rx-lite';

const propTypes = {
  params: React.PropTypes.object,
  nexusStream: React.PropTypes.object,
  logStream: React.PropTypes.object,
  graphStream: React.PropTypes.object,
  systemStream: React.PropTypes.object,
  stateStream: React.PropTypes.object,
  query: React.PropTypes.object,
  children: React.PropTypes.oneOfType([
    React.PropTypes.array,
    React.PropTypes.object
  ])
};

class App extends React.Component {

  constructor(props) {
      super(props);
      API.defaultOrLastTarget();
      this.state = {
        nexusStream: new Rx.Subject(),
        nexusObserver: new Rx.Subject(),
        stateStream: new Rx.BehaviorSubject(API.offline),
        graphStream: new Rx.ReplaySubject(100),
        logStream: new Rx.ReplaySubject(200),
        systemStream: new Rx.ReplaySubject(200)
    };
  }

  componentWillMount() {
      var thiz = this;
      API.ws('stream', this.state.stateStream).then(res => {
          res.receiver.subscribe(thiz.state.nexusStream);
          thiz.state.nexusObserver.subscribe(res.sender);
      }, e => {
          console.log(e);
      });
  }

  componentDidMount() {
  }

  componentWillUnmount() {
  }

  renderChildren() {
    return React.cloneElement(this.props.children, {
      params: this.props.params,
      query: this.props.query,
      currentUser: this.state.currentUser,
        nexusStream: this.state.nexusStream,
        nexusObserver: this.state.nexusObserver,
        stateStream: this.state.stateStream,
        graphStream: this.state.graphStream,
        logStream: this.state.logStream,
        systemStream: this.state.systemStream
    });
  }

  render() {
    return (
      <div>
        <Sidebar {...this.state} />
        <div id='main'>
          {this.renderChildren()}
        </div>
      </div>
    );
  }

}

App.propTypes = propTypes;

export default App;