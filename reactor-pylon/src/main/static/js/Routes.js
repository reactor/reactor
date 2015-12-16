'use strict';

import React                       from 'react';
import {Router, Route, IndexRoute} from 'react-router';
import CreateBrowserHistory        from 'history/lib/createBrowserHistory';

import App                         from './App';
import Dashboard                   from './pages/Dashboard';
import Config                     from './pages/Config';
import Metrics                     from './pages/Metrics';
import Streams                     from './pages/Streams';
import Buffers                       from './pages/Buffers';
import Hosts                       from './pages/Hosts';
import Logs                        from './pages/Logs';
import System                      from './pages/System';
import Studio                      from './pages/Studio';
import NotFoundPage                from './pages/NotFoundPage';
import API                          from './services/NexusService';

export default (
  <Router history={CreateBrowserHistory()}>

    <Route prefix="/pylon" component={App}>
      <IndexRoute component={Dashboard}/>
      <Route path="/pylon/connect" component={Config}  />
      <Route path="/pylon/dashboard" component={Dashboard} onEnter={API.checkConnection}/>
      <Route path="/pylon/metrics" component={Metrics} onEnter={API.checkConnection}/>
      <Route path="/pylon/streams" component={Streams} onEnter={API.checkConnection}/>
      <Route path="/pylon/hosts" component={Hosts} onEnter={API.checkConnection}/>
      <Route path="/pylon/buffers" component={Buffers} onEnter={API.checkConnection}/>
      <Route path="/pylon/system" component={System} onEnter={API.checkConnection}/>
      <Route path="/pylon/logs" component={Logs} onEnter={API.checkConnection}/>
      <Route path="/pylon/studio" component={Studio} />
      <Route path="/pylon" component={Dashboard} onEnter={API.checkConnection} />
      <Route path="*" component={NotFoundPage} />
    </Route>

  </Router>
);
