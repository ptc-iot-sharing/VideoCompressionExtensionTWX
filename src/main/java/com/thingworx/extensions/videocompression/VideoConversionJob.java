package com.thingworx.extensions.videocompression;

import com.thingworx.types.collections.ValueCollection;
import com.thingworx.types.primitives.DatetimePrimitive;
import com.thingworx.types.primitives.StringPrimitive;
import org.joda.time.DateTime;

import java.nio.file.Path;

public class VideoConversionJob {
    private Path inputPath, outputPath;
    private String sourceFileRepo, targetFileRepo;
    private String sourceFilePath, targetFilePath;
    private String id;
    private DateTime addedTimestamp, startedTimestamp, finishedTimestamp;
    private String arguments;
    private FileConversionStatus status;

    public VideoConversionJob(Path inputPath, Path outputPath, String sourceFileRepo, String targetFileRepo,
                              String sourceFilePath, String targetFilePath, String id, String arguments) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.sourceFileRepo = sourceFileRepo;
        this.targetFileRepo = targetFileRepo;
        this.sourceFilePath = sourceFilePath;
        this.targetFilePath = targetFilePath;
        this.arguments = arguments;
        addedTimestamp = DateTime.now();
        this.id = id;
        status = FileConversionStatus.QUEUED;
    }

    public void startedProcessing() {
        startedTimestamp = DateTime.now();
        status = FileConversionStatus.IN_PROGRESS;
    }

    public void finishedProcessing() {
        finishedTimestamp = DateTime.now();
        status = FileConversionStatus.FINISHED;
    }

    public void errorInProcessing() {
        finishedTimestamp = DateTime.now();
        status = FileConversionStatus.ERROR;
    }

    public void canceledProcessing() {
        finishedTimestamp = DateTime.now();
        status = FileConversionStatus.CANCELED;
    }

    public Path getInputPath() {
        return inputPath;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public DateTime getAddedTimestamp() {
        return addedTimestamp;
    }

    public DateTime getStartedTimestamp() {
        return startedTimestamp;
    }

    public DateTime getFinishedTimestamp() {
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

    public ValueCollection toValueCollection() {
        ValueCollection vc = new ValueCollection();
        try {
            vc.setValue("jobId", new StringPrimitive(id));
            vc.setValue("inputRepo", new StringPrimitive(sourceFileRepo));
            vc.setValue("inputPath", new StringPrimitive(sourceFilePath));
            vc.setValue("outputRepo", new StringPrimitive(targetFileRepo));
            vc.setValue("outputPath", new StringPrimitive(targetFilePath));
            if (addedTimestamp != null) {
                vc.setValue("addedTimestamp", new DatetimePrimitive(addedTimestamp));
            }
            if (startedTimestamp != null) {
                vc.setValue("startedTimestamp", new DatetimePrimitive(startedTimestamp));
            }
            if (finishedTimestamp != null) {
                vc.setValue("finishedTimestamp", new DatetimePrimitive(finishedTimestamp));
            }
            vc.setValue("arguments", new StringPrimitive(arguments));
            vc.setValue("status", new StringPrimitive(status.toString()));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return vc;
    }

    public String getId() {
        return id;
    }

    public String getArguments() {
        return arguments;
    }

    public enum FileConversionStatus {
        QUEUED, IN_PROGRESS, FINISHED, ERROR, CANCELED;
    }
}
