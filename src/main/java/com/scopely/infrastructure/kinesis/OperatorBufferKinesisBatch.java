package com.scopely.infrastructure.kinesis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import rx.Observable;
import rx.Producer;
import rx.Subscriber;

/**
 * A version of {@link rx.internal.operators.OperatorBufferWithSize} which buffers {@link ByteBuffer}s
 * that don't exceeded the configured count and size.
 */
public class OperatorBufferKinesisBatch implements Observable.Operator<List<ByteBuffer>, ByteBuffer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OperatorBufferKinesisBatch.class);

    final int maxCount;
    final int maxSize;

    /**
     * @param maxCount the number of elements a buffer should have before being emitted
     * @param maxSize  max size in bytes of each list of ByteBuffers.
     */
    public OperatorBufferKinesisBatch(int maxCount, int maxSize) {
        if (maxCount <= 0) {
            throw new IllegalArgumentException("maxCount must be greater than 0");
        }
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be greater than 0");
        }
        this.maxCount = maxCount;
        this.maxSize = maxSize;
    }

    @Override
    public Subscriber<? super ByteBuffer> call(final Subscriber<? super List<ByteBuffer>> child) {
        return new Subscriber<ByteBuffer>(child) {
            List<ByteBuffer> buffer;
            AtomicInteger totalBufferSize;

            @Override
            public void setProducer(final Producer producer) {
                child.setProducer(new Producer() {

                    private volatile boolean infinite = false;

                    @Override
                    public void request(long n) {
                        if (infinite) {
                            return;
                        }
                        if (n >= Long.MAX_VALUE / maxCount) {
                            // n == Long.MAX_VALUE or n * maxCount >= Long.MAX_VALUE
                            infinite = true;
                            producer.request(Long.MAX_VALUE);
                        } else {
                            producer.request(n * maxCount);
                        }
                    }
                });
            }

            @Override
            public void onNext(ByteBuffer bytes) {
                if (buffer == null) {
                    buffer = new ArrayList<>(maxCount);
                    totalBufferSize = new AtomicInteger();
                }

                int currentBufferSize = bytes.limit();
                boolean reachedSizeLimit = totalBufferSize.get() + currentBufferSize > maxSize;

                if (reachedSizeLimit && buffer.isEmpty()) {
                    LOGGER.warn("Dropping single ByteBuffer which was too big for the configured max size.");
                    return;
                }

                if (!reachedSizeLimit) {
                    totalBufferSize.addAndGet(currentBufferSize);
                    buffer.add(bytes);
                }

                if (reachedSizeLimit || buffer.size() == maxCount) {
                    List<ByteBuffer> oldBuffer = buffer;
                    buffer = null;
                    child.onNext(oldBuffer);
                }
            }

            @Override
            public void onError(Throwable e) {
                buffer = null;
                totalBufferSize = null;
                child.onError(e);
            }

            @Override
            public void onCompleted() {
                List<ByteBuffer> oldBuffer = buffer;
                buffer = null;
                if (oldBuffer != null) {
                    try {
                        child.onNext(oldBuffer);
                    } catch (Throwable t) {
                        onError(t);
                        return;
                    }
                }
                child.onCompleted();
            }
        };
    }
}
