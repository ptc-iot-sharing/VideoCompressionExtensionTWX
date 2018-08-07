package com.thingworx.extensions.videocompression;

import ch.qos.logback.classic.Logger;
import com.thingworx.datashape.DataShape;
import com.thingworx.entities.utils.EntityUtilities;
import com.thingworx.entities.utils.ThingUtilities;
import com.thingworx.logging.LogUtilities;
import com.thingworx.metadata.annotations.*;
import com.thingworx.persistence.TransactionFactory;
import com.thingworx.relationships.RelationshipTypes;
import com.thingworx.security.context.SecurityContext;
import com.thingworx.things.Thing;
import com.thingworx.things.events.ThingworxEvent;
import com.thingworx.things.repository.FileRepositoryThing;
import com.thingworx.types.InfoTable;
import com.thingworx.types.collections.ValueCollection;
import com.thingworx.types.primitives.DatetimePrimitive;
import com.thingworx.types.primitives.IPrimitiveType;
import com.thingworx.types.primitives.InfoTablePrimitive;
import com.thingworx.types.primitives.StringPrimitive;
import com.thingworx.types.primitives.structs.VTQ;
import com.thingworx.webservices.context.ThreadLocalContext;
import org.joda.time.DateTime;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;

@ThingworxPropertyDefinitions(
        properties = {
                @ThingworxPropertyDefinition(
                        name = "ProcessingQueue",
                        description = "List of processing jobs to be converted",
                        baseType = "INFOTABLE",
                        aspects = {"isPersistent:false", "isReadOnly:true", "dataShape:VideoProcessingQueueDataShape"}
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
                        dataShape = "VideoProcessingQueueDataShape"
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
                                description = "Default arguments the conversion is run with",
                                baseType = "STRING",
                                aspects = {"friendlyName:FFmpeg default arguments", "defaultValue:-movflags faststart -vcodec h264 -acodec aac"}
                        )
                        }
                )
        )}
)
public class VideoCompressionThing extends Thing {
    private static final Logger LOGGER = LogUtilities.getInstance().getApplicationLogger(VideoCompressionThing.class);

    private ExecutorService executor;
    private FFmpegCliExecutor ffmpegCliExecutor;
    private Map<VideoConversionJob, Future> futureMap;

    @Override
    protected void initializeThing() throws Exception {
        super.initializeThing();
        executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "VideoCompressorThread"));
        ffmpegCliExecutor = new FFmpegCliExecutor(Paths.get(this.getStringConfigurationSetting(
                "ConfigurationInfo", "ffmpegPath")));
        futureMap = new ConcurrentHashMap<>();
    }

    @Override
    protected void processStartNotification() {
        super.processStartNotification();
        try {
            if (this.getPropertyValue("ProcessingQueue") == null) {
                DataShape datashapeEntity = (DataShape) EntityUtilities.findEntity("VideoProcessingQueueDataShape",
                        RelationshipTypes.ThingworxRelationshipTypes.DataShape);

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
            name = "CancelProcessingJob",
            description = "Cancels a processing job that is in queue or running",
            category = "VideoProcessing"
    )
    @ThingworxServiceResult(name = "result", baseType = "INFOTABLE", aspects = {"dataShape:VideoProcessingQueueDataShape"})
    public InfoTable CancelProcessingJob(
            @ThingworxServiceParameter(name = "jobId",
                    baseType = "STRING") String jobId) throws Exception {
        DataShape datashapeEntity = (DataShape) EntityUtilities.findEntity("VideoProcessingQueueDataShape",
                RelationshipTypes.ThingworxRelationshipTypes.DataShape);
        InfoTable result = datashapeEntity.CreateValues();
        for (VideoConversionJob videoConversionJob : futureMap.keySet()) {
            if (videoConversionJob.getId().equals(jobId)) {
                videoConversionJob.canceledProcessing();
                result.addRow(videoConversionJob.toValueCollection());
                futureMap.get(videoConversionJob).cancel(true);
                futureMap.remove(videoConversionJob);
                break;
            }
        }
        return result;
    }

    @ThingworxServiceDefinition(
            name = "AddVideoProcessingJob",
            description = "Adds a new video processing job to the queue",
            category = "VideoProcessing"
    )
    @ThingworxServiceResult(name = "result", baseType = "INFOTABLE", aspects = {"dataShape:VideoProcessingQueueDataShape"})
    public InfoTable AddVideoProcessingJob(
            @ThingworxServiceParameter(name = "sourceFileRepo",
                    baseType = "THINGNAME", aspects = {"thingTemplate:FileRepository"}) String sourceFileRepo,
            @ThingworxServiceParameter(name = "sourceFilePath",
                    baseType = "STRING") String sourceFilePath,
            @ThingworxServiceParameter(name = "targetFileRepo",
                    baseType = "THINGNAME", aspects = {"thingTemplate:FileRepository"}) String targetFileRepo,
            @ThingworxServiceParameter(name = "targetFilePath",
                    baseType = "STRING") String targetFilePath,
            @ThingworxServiceParameter(name = "arguments",
                    baseType = "STRING") String arguments) throws Exception {
        Path inputPath = getRealFilePath(sourceFileRepo, sourceFilePath);
        Path outputPath = getRealFilePath(targetFileRepo, targetFilePath);
        String jobId = UUID.randomUUID().toString();
        InfoTable currentQueue = ((InfoTable) this.getPropertyValue("ProcessingQueue").getValue()).clone();
        if (arguments == null || "".equals(arguments)) {
            arguments = this.getStringConfigurationSetting("ConfigurationInfo", "ffmpegArguments");
        }
        VideoConversionJob job = new VideoConversionJob(inputPath, outputPath, sourceFileRepo, targetFileRepo,
                sourceFilePath, targetFilePath, jobId, arguments);

        currentQueue.addRow(job.toValueCollection());

        this.setPropertyVTQ("ProcessingQueue", new VTQ(new InfoTablePrimitive(currentQueue)),
                false, true, true);
        Future futureJob = executor.submit(() -> {
            try {
                job.startedProcessing();
                this.updateJobToState(job, VideoConversionJob.FileConversionStatus.IN_PROGRESS);
                ffmpegCliExecutor.runConversionJob(job);
                job.finishedProcessing();
                this.updateJobToState(job, VideoConversionJob.FileConversionStatus.FINISHED);
                fireAsyncEvent(job, "ProcessingFinished", "VideoProcessingQueueDataShape", "");
            } catch (Exception ex) {
                if (job.getStatus().equals(VideoConversionJob.FileConversionStatus.CANCELED)) {
                    try {
                        this.updateJobToState(job, VideoConversionJob.FileConversionStatus.CANCELED);
                        fireAsyncEvent(job, "ProcessingFailed", "VideoProcessingFailedEventDataShape", "User Canceled");
                    } catch (Exception e) {
                        LOGGER.error("Failed to update the job conversion status", jobId);
                    }
                } else {
                    job.errorInProcessing();
                    try {
                        this.updateJobToState(job, VideoConversionJob.FileConversionStatus.ERROR);
                        fireAsyncEvent(job, "ProcessingFailed", "VideoProcessingFailedEventDataShape", ex.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to update the job conversion status", jobId);
                    }
                }
            } finally {
                futureMap.remove(job);
            }
        });
        futureMap.put(job, futureJob);
        DataShape datashapeEntity = (DataShape) EntityUtilities.findEntity("VideoProcessingQueueDataShape",
                RelationshipTypes.ThingworxRelationshipTypes.DataShape);
        InfoTable result = datashapeEntity.CreateValues();
        result.addRow(job.toValueCollection());
        return result;
    }

    private void updateJobToState(VideoConversionJob job, VideoConversionJob.FileConversionStatus status) throws Exception {
        Optional<IPrimitiveType> processingQueue = this.getPropValueAsync("ProcessingQueue");
        if (processingQueue.isPresent()) {
            InfoTable currentQueue = ((InfoTable) processingQueue.get().getValue()).clone();
            for (ValueCollection row : currentQueue.getRows()) {
                if (row.getStringValue("jobId").equals(job.getId())) {
                    row.setValue("status", new StringPrimitive(status.toString()));
                    switch (status) {
                        case IN_PROGRESS:
                            row.setValue("startedTimestamp", new DatetimePrimitive(DateTime.now()));
                            break;
                        case ERROR:
                        case FINISHED:
                            row.setValue("finishedTimestamp", new DatetimePrimitive(DateTime.now()));
                            break;
                    }
                }
            }
            setPropValueAsync("ProcessingQueue", new VTQ(new InfoTablePrimitive(currentQueue)));
        }
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

    /**
     * Sets a property from a non-thingworx thread.
     * The property set happens in a SuperUserContext
     *
     * @param propName  property to set
     * @param propValue VTQ of the property
     */
    private void setPropValueAsync(String propName, VTQ propValue) {
        try {
            TransactionFactory.beginTransactionRequired();
            ThreadLocalContext.setSecurityContext(SecurityContext.createSuperUserContext());
            this.setPropertyVTQ(propName, propValue, false, true, false);
            ThreadLocalContext.setTransactionSuccess(true);
        } catch (Exception e) {
            LOGGER.error("Could not set property " + propName, e);
            ThreadLocalContext.setTransactionSuccess(false);
            TransactionFactory.failure();
        } finally {
            TransactionFactory.endTransactionRequired();
            ThreadLocalContext.cleanupContext();
        }
    }

    /**
     * Gets a property from a non-thingworx thread.
     * The property set happens in a SuperUserContext
     *
     * @param propName property to get
     * @return Value of the property
     */
    private Optional<IPrimitiveType> getPropValueAsync(String propName) {
        Optional<IPrimitiveType> value = Optional.empty();
        try {
            TransactionFactory.beginTransactionRequired();
            ThreadLocalContext.setSecurityContext(SecurityContext.createSuperUserContext());
            value = Optional.ofNullable(this.getPropertyValue(propName));
            ThreadLocalContext.setTransactionSuccess(true);
        } catch (Exception e) {
            LOGGER.error("Could not get property " + propName, e);
            ThreadLocalContext.setTransactionSuccess(false);
            TransactionFactory.failure();
        } finally {
            TransactionFactory.endTransactionRequired();
            ThreadLocalContext.cleanupContext();
        }
        return value;
    }

    /**
     * Fires an event from a non-thingworx thread
     * The event happens in a SuperUserContext
     *
     * @param job job that triggered the event
     */
    private void fireAsyncEvent(VideoConversionJob job, String eventName, String eventDataShape, String eventDetails) {
        try {
            SecurityContext e = SecurityContext.createSuperUserContext();
            try {
                ThingworxEvent event = new ThingworxEvent();
                event.setTraceActive(ThreadLocalContext.isTraceActive());
                event.setSecurityContext(e);
                event.setSource(this.getName());
                event.setEventName(eventName);
                DataShape datashapeEntity = (DataShape) EntityUtilities.findEntityDirect(eventDataShape,
                        RelationshipTypes.ThingworxRelationshipTypes.DataShape);
                InfoTable data = datashapeEntity.CreateValues();
                ValueCollection vc = job.toValueCollection();
                vc.setValue("message", new StringPrimitive(eventDetails));
                data.addRow(vc);
                event.setEventData(data);
                if (Thing._logger.isDebugEnabled()) {
                    Thing._logger.debug("************** DISPATCHING " + eventName + " EVENT on behalf of " + this.toString()
                            + " " + this.getName());
                }

                this.dispatchBackgroundEvent(event);
                if (Thing._logger.isDebugEnabled()) {
                    Thing._logger.debug("************** DONE DISPATCHING " + eventName + " EVENT on behalf of "
                            + this.toString() + " " + this.getName());
                }
            } catch (Exception ex) {
                throw new Exception(eventName + " Event Failure : " + this.getName(), ex);
            }
        } catch (Exception e) {
            Thing._logger.error("************** " + eventName + " EVENT ERROR on behalf of " +
                    this.toString() + " " + this.getName() + " : " + e.getMessage());
            LogUtilities.logInstanceExceptionDetails(this.getName(), this.getClass(), e);
        } finally {
            ThreadLocalContext.cleanupContext();
        }

    }

    @Override
    protected void stopThing() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
            executor.awaitTermination(10_000, TimeUnit.MILLISECONDS);
        }
    }
}
