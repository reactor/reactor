'use strict';

import gulp   from 'gulp';
import del    from 'del';
import config from '../config';

gulp.task('clean', function (cb) {

    del([config.buildDir], cb);

});