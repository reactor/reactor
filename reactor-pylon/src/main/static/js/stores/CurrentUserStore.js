'use strict';

import Reflux             from 'reflux';

import CurrentUserActions from '../actions/CurrentUserActions';
import AuthAPI            from '../utils/AuthAPI';

const CurrentUserStore = Reflux.createStore({

  init() {
    this.user = null;
    this.hasBeenChecked = false;

    this.listenTo(CurrentUserActions.checkLoginStatus, this.checkLoginStatus);
    this.listenTo(CurrentUserActions.login, this.loginUser);
    this.listenTo(CurrentUserActions.logout, this.logoutUser);
  },

  setUser(user) {
    this.user = user;
    this.trigger(null, this.user);
  },

  throwError(err) {
    this.trigger(err);
  },

  checkLoginStatus() {
    if ( this.user ) {
      this.setUser(this.user);
    } else {
      AuthAPI.checkLoginStatus().then(user => {
        this.hasBeenChecked = true;
        this.setUser(user);
      }).catch(err => {
        this.hasBeenChecked = true;
        this.throwError(err);
      });
    }
  },

  loginUser(user) {
    AuthAPI.login(user).then(user => {
      this.setUser(user);
    }).catch(err => {
      this.throwError(err);
    });
  },

  logoutUser() {
    AuthAPI.logout(this.user).then(() => {
      this.setUser(null);
    });
  }

});

export default CurrentUserStore;