import ch.qos.logback.classic.Level
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.ConsoleAppender

appender('stdout', ConsoleAppender) {
    Target = 'System.out'
    encoder(PatternLayoutEncoder) {
        pattern = '%d %p - %marker - %m%n'
    }
}

logger('logger', Level.DEBUG, ['stdout'])
