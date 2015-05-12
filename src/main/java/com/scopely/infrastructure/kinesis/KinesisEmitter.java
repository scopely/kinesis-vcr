package com.scopely.infrastructure.kinesis;

import com.amazonaws.services.kinesis.connectors.UnmodifiableBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;

import java.io.IOException;
import java.util.List;

public class KinesisEmitter implements IEmitter<byte[]> {
    @Override
    public List<byte[]> emit(UnmodifiableBuffer<byte[]> buffer) throws IOException {
        return null;
    }

    @Override
    public void fail(List<byte[]> records) {

    }

    @Override
    public void shutdown() {

    }
}
