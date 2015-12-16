'use strict';

import gulp   from 'gulp';
import gulpif from 'gulp-if';
import config from '../config';

gulp.task('copyIcons', function () {

    // Copy icons from root directory to build/
    var stream = gulp.src([config.sourceDir + 'favicon.ico']);

    if (!global.isProd && config.devDir !== undefined) {
        stream.pipe(gulp.dest(config.devDir));
    }

    return stream.pipe(gulp.dest(config.buildDir));
});
