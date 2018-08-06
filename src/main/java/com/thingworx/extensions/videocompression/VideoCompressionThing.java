package com.thingworx.extensions.videocompression;

import com.thingworx.datashape.DataShape;
import com.thingworx.entities.utils.EntityUtilities;
import com.thingworx.entities.utils.ThingUtilities;
import com.thingworx.metadata.annotations.*;
import com.thingworx.relationships.RelationshipTypes;
import com.thingworx.things.Thing;
import com.thingworx.things.repository.FileRepositoryThing;
import com.thingworx.types.InfoTable;
import com.thingworx.types.collections.ValueCollection;
import com.thingworx.types.primitives.DatetimePrimitive;
import com.thingworx.types.primitives.InfoTablePrimitive;
import com.thingworx.types.primitives.StringPrimitive;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@ThingworxPropertyDefinitions(
        properties = {
                @ThingworxPropertyDefinition(
                        name = "ProcessingQueue",
                        description = "List of processing jobs to be converted",
                        baseType = "INFOTABLE",
                        aspects = {"isPersistent:true", "isReadOnly:false", "dataShape:VideoProcessingQueueDataShape"}
                )
        }
)
@ThingworxEventDefinitions(
        events = {@ThingworxEventDefinition(
                name = "ProcessingFailed",
                description = "Video Processing Failed",
                dataShape = "VideoProcessingFailedEventDataShape"
        ),
                @ThingworxEventDefinition(
                        name = "ProcessingFinished",
                        description = "Video Processing finished",
                        dataShape = "VideoProcessingFinishedEventDataShape"
                )}
)

@ThingworxConfigurationTableDefinitions(
        tables = {@ThingworxConfigurationTableDefinition(
                name = "ConfigurationInfo",
                description = "Video Configuration Parameters",
                dataShape = @ThingworxDataShapeDefinition(
                        fields = {@ThingworxFieldDefinition(
                                ordinal = 0,
                                name = "ffmpegPath",
                                description = "Local path to the ffmpeg binary",
                                baseType = "STRING",
                                aspects = {"friendlyName:ffmpeg path"}
                        ), @ThingworxFieldDefinition(
                                ordinal = 1,
                                name = "ffmpegArguments",
                                description = "Arguments the conversion is run with",
                                baseType = "STRING",
                                aspects = {"friendlyName:FFmpeg arguments", "defaultValue:-movflags faststart -vcodec h264 -acodec aac"}
                        ), @ThingworxFieldDefinition(
                                ordinal = 2,
                                name = "maxQueueSize",
                                description = "Maximum number of elements to store in the queue",
                                baseType = "INTEGER",
                                aspects = {"friendlyName:MaxItems", "defaultValue:500"}
                        )
                        }
                )
        )}
)
public class VideoCompressionThing extends Thing {

    private ExecutorService executor;
    private FFmpegCliExecutor ffmpegCliExecutor;

    @Override
    protected void initializeThing() throws Exception {
        super.initializeThing();
        executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "VideoCompressorThread"));
        ffmpegCliExecutor = new FFmpegCliExecutor(Paths.get(this.getStringConfigurationSetting(
                "ConfigurationInfo", "ffmpegPath")),
                this.getStringConfigurationSetting("ConfigurationInfo", "ffmpegArguments"));
    }

    @Override
    protected void processStartNotification() {
        super.processStartNotification();
        try {
            if (this.getPropertyValue("ProcessingQueue") == null) {
                DataShape datashapeEntity = (DataShape) EntityUtilities.findEntity("VideoProcessingQueueDataShape",
                        RelationshipTypes.ThingworxRelationshipTypes.DataShapes);

                this.setPropertyValue("ProcessingQueue",
                        new InfoTablePrimitive(datashapeEntity.CreateValues()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @ThingworxServiceDefinition(
            name = "GetFFmpegVersion",
            description = "Validates if the ffmpeg path is correctly configured",
            category = "VideoProcessing"
    )
    @ThingworxServiceResult(name = "result", baseType = "STRING")
    public String GetFFmpegVersion() throws Exception {
        return ffmpegCliExecutor.getVersion();
    }

    @ThingworxServiceDefinition(
            name = "AddVideoProcessingJob",
            description = "Adds a new video processing job to the queue",
            category = "VideoProcessing"
    )
    public void AddVideoProcessingJob(
            @ThingworxServiceParameter(name = "sourceFileRepo",
                    baseType = "THINGNAME", aspects = {"thingTemplate:FileRepository"}) String sourceFileRepo,
            @ThingworxServiceParameter(name = "sourceFilePath",
                    baseType = "STRING") String sourceFilePath,
            @ThingworxServiceParameter(name = "targetFileRepo",
                    baseType = "THINGNAME", aspects = {"thingTemplate:FileRepository"}) String targetFileRepo,
            @ThingworxServiceParameter(name = "targetFilePath",
                    baseType = "STRING") String targetFilePath) throws Exception {
        Path inputPath = getRealFilePath(sourceFileRepo, sourceFilePath);
        Path outputPath = getRealFilePath(targetFileRepo, targetFilePath);
        String jobId = UUID.randomUUID().toString();
        InfoTable currentQueue = ((InfoTable) this.getPropertyValue("ProcessingQueue").getValue()).clone();
        ValueCollection vc = new ValueCollection();
        vc.setValue("jobId", new StringPrimitive(jobId));
        vc.setValue("inputRepo", new StringPrimitive(sourceFileRepo));
        vc.setValue("inputPath", new StringPrimitive(sourceFileRepo));
        vc.setValue("outputRepo", new StringPrimitive(sourceFileRepo));
        vc.setValue("outputPath", new StringPrimitive(sourceFileRepo));
        vc.setValue("addedTimestamp", new DatetimePrimitive(Instant.now().getEpochSecond()));
        vc.setValue("status", new StringPrimitive(VideoConversionJob.FileConversionStatus.QUEUED.toString()));
        currentQueue.addRow(vc);

        this.setPropertyValue("ProcessingQueue", new InfoTablePrimitive(currentQueue));

        VideoConversionJob job = new VideoConversionJob(inputPath, outputPath);
        Future futureJob = executor.submit(() -> {
            try {
                job.startedProcessing();
                ffmpegCliExecutor.runConversionJob(job);
                job.finishedProcessing();
            } catch (Exception ex) {
                job.errorInProcessing();
            }
        });
    }

    private Path getRealFilePath(String fileRepository, String filePath) throws Exception {
        // try to find the file to convert
        Thing thing = ThingUtilities.findThing(fileRepository);
        if (thing == null) {
            throw new Exception("Thing " + fileRepository + " cannot be found");
        }
        if (!thing.getImplementedThingTemplates().contains("FileRepository")) {
            throw new Exception("Thing " + fileRepository + " is not a FileRepository");
        }
        FileRepositoryThing fileRepositoryThing = ((FileRepositoryThing) thing);
        return Paths.get(fileRepositoryThing.getRootPath(), filePath);
    }

    @Override
    protected void stopThing() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(10_000, TimeUnit.MILLISECONDS);
        }
    }
}
