'use strict';

import gulp   from 'gulp';
import config from '../config';

gulp.task('copyFonts', function() {

  return gulp.src([config.sourceDir + 'fonts/**/*'])
    .pipe(gulp.dest(config.buildDir + 'fonts/'));

});
