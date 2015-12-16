'use strict';

import url         from 'url';
import browserSync from 'browser-sync';
import gulp        from 'gulp';
import config      from '../config';

gulp.task('browserSync', function () {

    const DEFAULT_FILE = 'index.html';
    const ASSET_EXTENSIONS = ['js', 'css', 'png', 'jpg', 'jpeg', 'gif'];

    browserSync.init({
        server: {
            baseDir: config.buildDir, middleware: function (req, res, next) {
                let fileHrefArray = url.parse(req.url).href.split('.');
                let fileExtension = fileHrefArray[fileHrefArray.length - 1];

                if (ASSET_EXTENSIONS.indexOf(fileExtension) === -1) {
                    req.url = '/' + DEFAULT_FILE;
                }

                return next();
            }
        }, port: config.browserPort, ui: {
            port: config.UIPort
        }, ghostMode: {
            links: false
        }
    });

});