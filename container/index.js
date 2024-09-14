const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} = require("@aws-sdk/client-s3");
const fs = require("node:fs/promises");
const fsOld = require("node:fs");
const path = require("node:path");
const ffmpeg = require("fluent-ffmpeg");

const RESOLUTIONS = [
  { name: "360p", width: 480, height: 360 },
  { name: "480p", width: 858, height: 480 },
  { name: "720p", width: 1280, height: 720 },
];

const s3Client = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// ENV
const BUCKET = process.env.BUCKET_NAME;
const KEY = process.env.KEY;

// Main function to handle video download, transcoding, and upload
async function init() {
  // 1. Download the original video from S3
  const command = new GetObjectCommand({
    Bucket: BUCKET, // Use the bucket specified in .env
    Key: KEY, // Use the key (filename) specified in .env
  });

  const result = await s3Client.send(command); // Fetch video from S3
  const originalFilePath = `original-video.mp4`; // Temporary local filename for the video
  await fs.writeFile(originalFilePath, result.Body); // Write the video to local file system

  const originalVideoPath = path.resolve(originalFilePath); // Get absolute path of the video

  // 2. Start the transcoding process for each resolution in the RESOLUTIONS array
  const promises = RESOLUTIONS.map((resolution) => {
    const output = `video-${resolution.name}.mp4`; // Output filename for each resolution

    return new Promise((resolve) => {
      ffmpeg(originalVideoPath) // Input video for transcoding
        .output(output) // Set output file
        .videoCodec("libx264") // Video codec for compression
        .audioCodec("aac") // Audio codec
        .withSize(`${resolution.width}x${resolution.height}`) // Set the video resolution
        .on("end", async () => {
          // When transcoding finishes
          // 3. Upload the transcoded video to S3
          const putCommand = new PutObjectCommand({
            Bucket: process.env.UPLOAD_BUCKET_NAME, // Destination bucket name from .env
            Key: output, // Use the generated output filename
            Body: fsOld.createReadStream(path.resolve(output)), // Create read stream for uploading
          });

          await s3Client.send(putCommand); // Upload to S3
          console.log(`Uploaded ${output}!`); // Log success message
          resolve(output); // Resolve promise when upload is complete
        })
        .format("mp4") // Ensure the output is in MP4 format
        .run(); // Start the transcoding process
    });
  });

  // 4. Wait for all transcoding and uploads to complete
  await Promise.all(promises);
}

// Start the process
init();
