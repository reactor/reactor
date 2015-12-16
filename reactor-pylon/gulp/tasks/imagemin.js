'use strict';

import gulp        from 'gulp';
import gulpif      from 'gulp-if';
import browserSync from 'browser-sync';
import imagemin    from 'gulp-imagemin';
import config      from '../config';

gulp.task('imagemin', function () {

    // Run imagemin task on all images
    var stream = gulp.src(config.images.src)
        .pipe(gulpif(global.isProd, imagemin()));

    if (!global.isProd && config.devDir !== undefined) {
        stream.pipe(gulp.dest(config.devDir + 'assets/images'));
    }

    return stream.pipe(gulp.dest(config.images.dest)).pipe(
        gulpif(browserSync.active, browserSync.reload({stream: true, once: true})));
});