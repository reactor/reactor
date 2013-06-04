# Disruptor-based AsyncAppender for Logback

The `reactor-logback` module is somewhat misleadingly-named. There are no Reactor dependencies in it. It uses the LMAX Disruptor RingBuffer directly to provide high-speed, asynchronous logging for applications that don't want to give up logging just because it kills the performance of your async application.

### Maven artifacts

The Maven artifacts are currently in the snapshot repo. To use them, you'll need to add a reference to the snapshot repo in your build file. For Gradle, that would be something like:

    ext {
      reactorVersion = '1.0.0.BUILD-SNAPSHOT'
    }

    repositories {
      mavenLocal()
      mavenCentral()
      maven { url 'http://repo.springsource.org/libs-snapshot' }
    }

    dependencies {
      runtime "reactor:reactor-logback:$reactorVersion"
    }

### Configuration

You use the AsyncAppender like any normal Logback appender. You can direct any logger to it and that logger's output will be written asynchronously. To configure Logback to do fully async logging for everything, just declare the AsyncAppender like the following (in `logback.xml`):

    <configuration>

      <!-- The underlying appender will be the standard console one. -->
      <appender name="stdout" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
          <pattern>
            %d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n
          </pattern>
        </encoder>
      </appender>

      <!-- Wrap calls to the console logger with async dispatching to Disruptor. -->
      <appender name="async" class="reactor.logback.AsyncAppender">
        <appender-ref ref="stdout"/>
      </appender>

      <!-- Direct all logging through the AsyncAppender. -->
      <root level="info">
        <appender-ref ref="async"/>
      </root>

    </configuration>

---

Reactor is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
