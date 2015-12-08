'use strict';

import gulp   from 'gulp';
import gulpif from 'gulp-if';
import config from '../config';

gulp.task('copyIcons', function () {

    // Copy icons from root directory to build/
    return gulp.src([config.sourceDir + 'favicon.ico'])
        .pipe(gulp.dest(config.buildDir))
        .pipe(gulpif(typeof config.devDir !== 'undefined', gulp.dest(config.devDir)))

});
