package com.thingworx.extensions.videocompression;

import ch.qos.logback.classic.Logger;
import com.thingworx.logging.LogUtilities;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FFmpegCliExecutor {

    private static final Logger LOGGER = LogUtilities.getInstance().getApplicationLogger(FFmpegCliExecutor.class);

    private Path ffmpegPath;

    public FFmpegCliExecutor(Path ffmpegPath) {
        this.ffmpegPath = ffmpegPath;
    }

    public void runConversionJob(VideoConversionJob job) throws IOException {
        LOGGER.debug("Starting execution of job " + job.getId() + " converting file " +
                job.getInputPath());
        List<String> commandArgs = new ArrayList<>();
        commandArgs.add(ffmpegPath.resolve(Paths.get("ffmpeg")).toString());
        commandArgs.addAll(Arrays.asList("-i", job.getInputPath().toString()));
        commandArgs.add("-y");
        commandArgs.addAll(Arrays.asList(job.getArguments().split(" ")));
        commandArgs.add(job.getOutputPath().toString());

        Process process = new ProcessBuilder(commandArgs.toArray(new String[0]))
                .redirectErrorStream(true)
                .start();
        BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        CircularRingBuffer<String> lastLines = new CircularRingBuffer<>(10);
        while ((line = input.readLine()) != null) {
            lastLines.add(line);
            LOGGER.trace("FFmpeg job id " + job.getId() + " " + line);
            if (Thread.currentThread().isInterrupted()) {
                LOGGER.warn("Killing ffmepg as thingworx is shutting down");
                process.destroy();
                break;
            }
        }
        if (process.exitValue() != 0) {
            // get the last line
            throw new RuntimeException(String.format("FFmpeg process existed with status %d. Last outputs: %s",
                    process.exitValue(), String.join("\n ", lastLines.getAll())));
        }
        LOGGER.debug("Finished execution of job " + job.getId());
    }

    public String getVersion() throws IOException {
        Process process = new ProcessBuilder(ffmpegPath.resolve(Paths.get("ffmpeg")).toString(), "-version")
                .redirectErrorStream(true)
                .start();
        BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        StringBuilder builder = new StringBuilder();
        while ((line = input.readLine()) != null) {
            builder.append(line);
            builder.append('\n');
        }
        return builder.toString();
    }

}
