'use strict';

import gulp         from 'gulp';
import compass      from 'gulp-compass';
import concat      from 'gulp-concat';
import gulpif       from 'gulp-if';
import browserSync  from 'browser-sync';
import autoprefixer from 'gulp-autoprefixer';
import handleErrors from '../util/handle-errors';
import config       from '../config';

gulp.task('sass', function () {
    return gulp.src(config.styles.src)
        /*.pipe(sass({
         sourceComments: global.isProd ? 'none' : 'map',
         sourceMap: 'sass',
         outputStyle: global.isProd ? 'compressed' : 'nested'
         }))*/
        .pipe(compass({
            sass: config.sourceDir + 'styles',
            image: config.sourceDir + 'images',
            css: config.styles.dest,
            sourceMap: 'sass'
        }))
        .on('error', handleErrors)
        .pipe(autoprefixer('last 2 versions', '> 1%', 'ie 8'))
        .pipe(gulpif(typeof config.devDir !== 'undefined', gulp.dest(config.devDir + 'assets/css')))
        .pipe(gulp.dest(config.styles.dest));
});
