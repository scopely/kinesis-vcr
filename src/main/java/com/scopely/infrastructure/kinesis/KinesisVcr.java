package com.scopely.infrastructure.kinesis;

public class KinesisVcr {

    public static void main(String[] args) {
        VcrConfiguration vcrConfiguration = new VcrConfiguration(System.getenv());
        vcrConfiguration.validateConfiguration();

        if (args.length > 0 && "play".equals(args[0])) {
            KinesisPlayer player = new KinesisPlayer(vcrConfiguration);
            player.run();
        } else {
            KinesisRecorder recorder = new KinesisRecorder(vcrConfiguration);
            recorder.run();
        }
    }
}
