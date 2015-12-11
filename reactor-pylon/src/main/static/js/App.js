'use strict';

import React              from 'react';

import Sidebar            from './components/Sidebar';
import Config             from './pages/Config';
import routes             from './Routes';
import API                from './services/NexusService';
import Rx                 from 'rx-lite';
import ReactDOM           from 'react-dom';

const propTypes = {
  params: React.PropTypes.object,
  nexusStream: React.PropTypes.object,
  logStream: React.PropTypes.object,
  graphStream: React.PropTypes.object,
  systemStream: React.PropTypes.object,
  stateStream: React.PropTypes.object,
  configuration: React.PropTypes.object,
  children: React.PropTypes.oneOfType([
    React.PropTypes.array,
    React.PropTypes.object
  ])
};

class App extends React.Component {

  constructor(props, context) {
      super(props, context);
      var nexusStream = new Rx.Subject();
      var graphStream = new Rx.ReplaySubject(100);
      var systemStream = new Rx.ReplaySubject(200);
      var logStream = new Rx.ReplaySubject(200);

      nexusStream
          .filter(json => json.type == "GraphEvent")
          .subscribe(graphStream);

      nexusStream
          .filter(json => json.type == "SystemEvent")
          .subscribe(systemStream);

      nexusStream
          .filter(json => json.type == "LogEvent")
          .subscribe(logStream);

      var startCB = this.start.bind(this);

      this.state = {
        disposable: null,
        nexusStream: nexusStream,
        nexusObserver: new Rx.Subject(),
        stateStream: new Rx.BehaviorSubject(API.offline),
        configuration: null,
        graphStream: graphStream,
        logStream: logStream,
        systemStream: systemStream,
          connect: startCB
    };
  }

  componentWillMount() {

  }

    onConnectError(e){
        console.log(e);
    }

    onConnect(res) {
        if(this.state.disposable == null) {
            this.setState({
                disposable: res.receiver.subscribe(this.state.nexusStream)
            });
            this.state.nexusObserver.subscribe(res.sender);
        }

        const { location } = this.props;

        if (location.state && location.state.nextPathname) {
            routes.props.history.replaceState(null, location.state.nextPathname)
        } else {
            routes.props.history.replaceState(null, location.pathname)
        }
    }

    start(apiURL){
       API.ws(apiURL, this.state.stateStream).then(this.onConnect.bind(this), this.onConnectError.bind(this));

        //ReactDOM.render (this.renderChildren(), document.getElementById('main'));
    }
  componentDidMount() {
      this.start(API.defaultTarget)
  }

  componentWillUnmount() {
      if(this.state.disposable != null){
          this.state.nexusObservable.dispose();
          this.state.disposable.dispose();
      }
  }

  renderChildren() {
    return React.cloneElement(this.props.children, {
      params: this.props.params,
      nexusStream: this.state.nexusStream,
      connect: this.state.connect,
      nexusObserver: this.state.nexusObserver,
      stateStream: this.state.stateStream,
      graphStream: this.state.graphStream,
      logStream: this.state.logStream,
      systemStream: this.state.systemStream,
      configuration: this.state.configuration
    });
  }

    render(){
        return (
            <div>
                <Sidebar {...this.state} />
                <div id='main'>
                    {this.renderChildren()}
                </div>
            </div>
        )
    }

}
App.propTypes = propTypes;

export default App;