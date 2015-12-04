'use strict';

import gulp        from 'gulp';
import gulpif      from 'gulp-if';
import browserSync from 'browser-sync';
import imagemin    from 'gulp-imagemin';
import config      from '../config';

gulp.task('imagemin', function () {

    // Run imagemin task on all images
    return gulp.src(config.images.src)
        .pipe(gulpif(global.isProd, imagemin()))
        .pipe(gulp.dest(config.images.dest))
        .pipe(gulpif(config.devDir !== 'undefined', gulp.dest(config.devDir + 'assets/images')))
        .pipe(gulpif(browserSync.active, browserSync.reload({stream: true, once: true})));
});