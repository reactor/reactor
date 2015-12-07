'use strict';

import gulp         from 'gulp';
import gulpif       from 'gulp-if';
import gutil        from 'gulp-util';
import source       from 'vinyl-source-stream';
import streamify    from 'gulp-streamify';
import sourcemaps   from 'gulp-sourcemaps';
import rename       from 'gulp-rename';
import watchify     from 'watchify';
import browserify   from 'browserify';
import babelify     from 'babelify';
import uglify       from 'gulp-uglify';
import browserSync  from 'browser-sync';
import debowerify   from 'debowerify';
import handleErrors from '../util/handle-errors';
import config       from '../config';

// Based on: http://blog.avisi.nl/2014/04/25/how-to-keep-a-fast-build-with-browserify-and-reactjs/
function buildScript(file, watch) {

    var bundler = browserify({
        entries: [config.sourceDir + 'js/' + file], debug: !global.isProd, cache: {}, packageCache: {}, fullPaths: false
    });

    if (watch) {
        bundler = watchify(bundler);
        bundler.on('update', rebundle);
    }

    bundler.transform(babelify);
    bundler.transform(debowerify);

    function rebundle() {
        let stream = bundler.bundle();

        gutil.log('Rebundle...');

        return stream.on('error', handleErrors)
            .pipe(source(file))
            //.pipe(gulpif(global.isProd, uglify()))
            .pipe(streamify(rename({
                basename: 'main'
            })))
            .pipe(gulpif(!global.isProd, sourcemaps.write('./')))
            .pipe(gulp.dest(config.scripts.dest))
            .pipe(gulpif(!global.isProd && config.devDir !== 'undefined', gulp.dest(config.devDir + 'assets/js')))
            .pipe(gulpif(browserSync.active, browserSync.reload({stream: true, once: true})));
    }

    return rebundle();

}

gulp.task('browserify', function () {

    // Only run watchify if NOT production
    return buildScript('index.js', !global.isProd);

});