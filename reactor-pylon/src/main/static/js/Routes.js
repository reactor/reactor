'use strict';

import React                       from 'react';
import {Router, Route, IndexRoute} from 'react-router';
import CreateBrowserHistory        from 'history/lib/createBrowserHistory';

import App                         from './App';
import Dashboard                   from './pages/Dashboard';
import Map                         from './pages/Map';
import Streams                     from './pages/Streams';
import Hosts                       from './pages/Hosts';
import System                      from './pages/System';
import NotFoundPage                from './pages/NotFoundPage';

export default (
  <Router history={CreateBrowserHistory()}>

    <Route prefix="/pylon" component={App}>
      <IndexRoute component={Dashboard}/>
      <Route path="/pylon/dashboard" component={Dashboard}  />
      <Route path="/pylon/map" component={Map} />
      <Route path="/pylon/streams" component={Streams}/>
      <Route path="/pylon/hosts" component={Hosts} />
      <Route path="/pylon/system" component={System} />
      <Route path="/pylon" component={Dashboard} />
      <Route path="*" component={NotFoundPage} />
    </Route>

  </Router>
);
