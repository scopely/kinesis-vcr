package com.scopely.infrastructure.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * Made (with love?) by Erik Price
 */
public class ExponentialBackoffRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExponentialBackoffRunner.class);

    @FunctionalInterface
    public interface CheckedExceptionTask<U> {
        U run() throws Throwable;
    }
    /**
     * Repeatedly run a task until it succeeds, or the specified timeout period
     * has been hit, whichever comes first.
     *
     * @param task Task to be run with retry
     * @param retryPredicate Predicate that evaluates true on throwables that we should retry
     * @param timeoutMillis Maximum time to wait for success
     */
    public static <T> Optional<T> run(CheckedExceptionTask<T> task, Predicate<Throwable> retryPredicate, long timeoutMillis) throws Throwable {
        final long endTime = System.currentTimeMillis() + timeoutMillis;
        for (int n = 0; System.currentTimeMillis() < endTime; ++n) {
            try {
                return Optional.of(task.run());
            } catch (Throwable e) {
                if (!retryPredicate.test(e)) {
                    // If we get some unknown kind of error, just rethrow
                    throw e;
                }
                long sleepTime = 1000 * (1 << n);
                // Make sure we don't oversleep too much
                if (System.currentTimeMillis() + sleepTime >= endTime) {
                    sleepTime = Math.max(endTime - System.currentTimeMillis(), 1);
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ex) {
                    LOGGER.info("Thread.sleep() interrupted", ex);
                }
            }
        }
        LOGGER.warn("No success after retry timeout expired");
        return Optional.empty();
    }
}
