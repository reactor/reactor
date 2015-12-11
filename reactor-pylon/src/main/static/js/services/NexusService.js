'use strict';

//import {camelizeKeys} from 'humps';
import Rx           from 'rx-lite';
import JSON           from 'JSON2';
import cookie           from 'react-cookie';

const NexusService = {

    defaultTarget: 'ws://localhost:12012/nexus/stream',
    target: '',

    offline: 0, ready: 1, working: 2, retry: 3,

    defaultOrLastTarget() {
        var target = cookie.load('targetAPI');
        if (typeof target === 'undefined') {
            this.target = this.defaultTarget;
        }
        else {
            this.target = target;
        }
        return this.target;
    },

    updateTargetAPI(target) {
        if (typeof target !== 'undefined') {
            this.target = target;
            cookie.save('targetAPI', this.target);
        }
    },

    bytesToSize(bytes) {
        var sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
        if (bytes == 0) return [0, "Bytes"];
        var i = parseInt(Math.floor(Math.log(bytes) / Math.log(1024)));
        return [Math.round(bytes / Math.pow(1024, i), 2), sizes[i]];
    },

    ws(path, stateObserver) {
        // Handle the data
        return new Promise((resolve, reject) => {

            if (stateObserver !== undefined) {
                stateObserver.onNext(this.working);
            }
            var ws = new WebSocket(path);

            var thiz = this;
            ws.onopen = (e) => {
                if (stateObserver !== undefined) {
                    stateObserver.onNext(thiz.ready);
                }

                resolve({
                    receiver: Rx.Observable.create ((obs) => {
                        // Handle messages
                        if (ws == null) {
                            ws = new WebSocket(path);
                            ws.onopen = (e) => {
                                if (stateObserver !== undefined) {
                                    stateObserver.onNext(thiz.ready);
                                }
                            };
                        }

                        ws.onmessage = (msg) => {
                            var data = JSON.parse(msg.data);
                            if (data.cause) {
                                obs.onError(data);
                            }
                            else {
                                obs.onNext(data);
                            }
                        };
                        ws.onerror = (e) => {
                            ws = null;
                            if (stateObserver !== undefined) {
                                stateObserver.onNext(thiz.offline);
                            }
                            obs.onError(e);
                        };

                        ws.onclose = (e) => {
                            if (ws != null) {
                                ws = null;
                                if (stateObserver !== undefined) {
                                    stateObserver.onNext(thiz.offline);
                                }
                                obs.onError({message: 'Server connection has been lost'});
                            }
                        };

                        // Return way to unsubscribe
                        return () => {
                            if (ws != null) {
                                console.log("closing connection");
                                var _ws = ws;
                                ws = null;
                                _ws.close();
                            }
                        }
                    }).retryWhen(attempts => {
                        return Rx.Observable
                            .range(1, 10)
                            .zip(attempts, i => i)
                            .scan(i => i * 2, 1)
                            .flatMap(i => {
                                if (stateObserver !== undefined) {
                                    stateObserver.onNext(thiz.retry);
                                }
                                console.log("delay retry by " + i + " second(s)");
                                return Rx.Observable.timer(i * 1000);
                            });
                    }), sender: Rx.Observer.create((data) => {
                        if (ws.readyState === WebSocket.OPEN) {
                            ws.send(data);
                        }
                    })
                });
            };

            ws.onerror = reject;
        });
    }
};

export default NexusService;