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
    private List<String> arguments;

    public FFmpegCliExecutor(Path ffmpegPath, String arguments) {
        this.ffmpegPath = ffmpegPath;
        this.arguments = Arrays.asList(arguments.split(" "));
    }

    public void runConversionJob(VideoConversionJob job) throws IOException {
        LOGGER.debug("Starting execution of job " + job.getId() + " converting file " +
                job.getInputPath());
        List<String> commandArgs = new ArrayList<>();
        commandArgs.add(ffmpegPath.resolve(Paths.get("ffmpeg")).toString());
        commandArgs.addAll(Arrays.asList("-i", job.getInputPath().toString()));
        commandArgs.add("-y");
        commandArgs.addAll(this.arguments);
        commandArgs.add(job.getOutputPath().toString());

        Process process = new ProcessBuilder(commandArgs.toArray(new String[0]))
                .redirectErrorStream(true)
                .start();
        BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;
        while ((line = input.readLine()) != null) {
            LOGGER.trace("FFmpeg job id " + job.getId() + " " + line);
            if (Thread.currentThread().isInterrupted()) {
                System.out.println("KILLING PROCESS");
                process.destroy();
                System.out.println("KILLING PROCESS1");

                break;
            }
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
