// Gulp plugins
var gulp = require('gulp'),
    gutil = require('gulp-util'),
    config = require('./configDefault'),
    merge = require('merge'),
    shell = require('gulp-shell'),
    runSequence = require('gulp-run-sequence'),
    clean = require('gulp-clean');

config = config.build;

// Cleaner for cleaning on a per-task basis
var copy = require('./tasks/copy')(config);

// Prepare prod build
gulp.task('production', function(){
    var configProduction = require('./configProduction');
    config = merge(config, configProduction);
});

// gradle wrapper
gulp.task('gradle', shell.task([
        './gradlew build'
    ])
);

gulp.task('clean:copy', function() {
    return gulp.src('copy').pipe(clean());
});

gulp.task('clean:scripts', function() {
    return gulp.src('scripts').pipe(clean());
});

gulp.task('clean:styles', function() {
    return gulp.src('styles').pipe(clean());
});

gulp.task('clean:fonts', function() {
    return gulp.src('fonts').pipe(clean());
});

gulp.task('cleanAll', ['clean:copy', 'clean:scripts', 'clean:styles', 'clean:scripts', 'clean:fonts']);

// Concat scripts (described in config)
gulp.task('scripts', ['clean:scripts'], require('./tasks/scripts')(config));

// Compile next css
gulp.task('styles', ['clean:styles'], require('./tasks/styles')(config));

// Copy static files that don't need any build processing to `public/dist`
gulp.task('copy:src', ['clean:copy'], copy("internal"));
gulp.task('copy:fontAwesome', ['clean:fonts'], copy ("awesome"));
gulp.task('copy', ['copy:src', 'copy:fontAwesome']);

// Bundle and watch for changes to all files appropriately
gulp.task('serve', ['scripts', 'copy', 'styles'], require('./tasks/serve')(config));

gulp.task('assemble', function(cb) {
    runSequence('cleanAll', 'production', 'scripts', 'copy', 'styles', cb);
});

// Environment
if (process.env.NODE_ENV == 'production') {
    gutil.log("Starting ", gutil.colors.yellow("Production environment"));

    gulp.task('package', function(cb) {
        runSequence('assemble', 'gradle', cb);
    });

    gulp.task('default', ['package']);
} else {
    gutil.log("Starting ", gutil.colors.yellow("Dev environment"));
    gulp.task('default', ['serve']);
}
