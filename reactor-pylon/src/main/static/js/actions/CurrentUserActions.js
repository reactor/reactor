'use strict';

import Reflux from 'reflux';

const CurrentUserActions = Reflux.createActions([

  'checkLoginStatus',
  'login',
  'logout'

]);

export default CurrentUserActions;