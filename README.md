# Video Compression Extension for Thingworx

This is a thingworx sdk extension that uses [ffmpeg](https://www.ffmpeg.org/) to compress video.

## Usage

### Initialization
1. Create a new thing using the `VideoConversionThingTemplate` template.
2. Go to configuration and provide the path to the folder with the ffmpeg binary
3. Provide the ffmpeg arguments that are used when user doesn't specify custom ones
4. Test the configuration by running the `GetFFmpegVersion` service.

### Runtime usage

Using the thing created above is quite simple. Only once conversion happens at a time. Two services are of importance:
* `AddVideoProcessingJob`: Creates a new processing job and add it to the queue. The service returns a infotable row with the job id. The following parameters are needed
    * `sourceFileRepo` - File repository where the file to convert is
    * `sourceFilePath` - Path to the file to convert
    * `targetFileRepo` - File repository where to store the converted file
    * `targetFilePath` - Path to the file to converted
    * `arguments`      - If provided, the arguments that are passed to ffmpeg for conversion. If not provided, the ones in the configuration are used.
* `CancelProcessingJob`:  Cancels a job, if queued or running.
    * `jobId`          - Job to cancel
    
When a job is completed, two events can trigger and be subscribed:
* `ProcessingFinished`: a job has finished, and the conversion was successful.
* `ProcessingFailed`: a job has finished but has thrown an error. The `message` column contains information about why it failed.

The `ProcessingQueue` property contains information about the jobs that have completed, are in queue, or are being processed.


#This Extension is provided as-is and without warranty or support. It is not part of the PTC product suite. This project is licensed under the terms of the MIT license  
