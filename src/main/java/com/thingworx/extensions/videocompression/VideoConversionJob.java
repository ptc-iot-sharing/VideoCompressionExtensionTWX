package com.thingworx.extensions.videocompression;

import org.joda.time.Instant;

import java.nio.file.Path;
import java.util.UUID;

public class VideoConversionJob {
    private Path inputPath, outputPath;
    private String id;
    private Instant addedTimestamp, startedTimestamp, finishedTimestamp;
    private FileConversionStatus status;

    public VideoConversionJob(Path inputPath, Path outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        addedTimestamp = Instant.now();
        id = UUID.randomUUID().toString();
        status = FileConversionStatus.QUEUED;
    }

    public void startedProcessing() {
        startedTimestamp = Instant.now();
        status = FileConversionStatus.IN_PROGRESS;
    }

    public void finishedProcessing() {
        finishedTimestamp = Instant.now();
        status = FileConversionStatus.FINISHED;
    }

    public void errorInProcessing() {
        finishedTimestamp = Instant.now();
        status = FileConversionStatus.ERROR;
    }

    public Path getInputPath() {
        return inputPath;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public Instant getAddedTimestamp() {
        return addedTimestamp;
    }

    public Instant getStartedTimestamp() {
        return startedTimestamp;
    }

    public Instant getFinishedTimestamp() {
        return finishedTimestamp;
    }

    public FileConversionStatus getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "VideoConversionJob{" +
                "inputPath=" + inputPath +
                ", outputPath=" + outputPath +
                ", addedTimestamp=" + addedTimestamp +
                ", startedTimestamp=" + startedTimestamp +
                ", finishedTimestamp=" + finishedTimestamp +
                ", status=" + status +
                '}';
    }

    public String getId() {
        return id;
    }

    public enum FileConversionStatus {
        QUEUED, IN_PROGRESS, FINISHED, ERROR;
    }
}
