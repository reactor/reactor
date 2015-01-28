# Disruptor-based AsyncAppender for Logback

The `reactor-logback` module uses the LMAX Disruptor RingBuffer via Reactor's `Processor` facility to provide high-speed, asynchronous logging for applications that don't want to give up logging just because it kills the performance of your async application.

### Maven artifacts

    ext {
      reactorVersion = '2.0.0.BUILD-SNAPSHOT'
    }

    repositories {
      maven { url 'http://repo.spring.io/libs-snapshot' }
      mavenCentral()
    }

    dependencies {
      runtime "io.projectreactor:reactor-logback:$reactorVersion"
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
