"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
require('dotenv').config(); // Load environment variables from .env file
const client_sqs_1 = require("@aws-sdk/client-sqs");
const client_ecs_1 = require("@aws-sdk/client-ecs");
// Initialize SQS client with credentials from environment variables
const client = new client_sqs_1.SQSClient({
    region: process.env.AWS_REGION, // AWS region from .env
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID, // AWS Access Key from .env
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY, // AWS Secret Access Key from .env
    },
});
// Initialize ECS client with credentials from environment variables
const ecsClient = new client_ecs_1.ECSClient({
    region: process.env.AWS_REGION, // AWS region from .env
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID, // AWS Access Key from .env
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY, // AWS Secret Access Key from .env
    },
});
function init() {
    return __awaiter(this, void 0, void 0, function* () {
        // Define command to receive messages from SQS queue
        const command = new client_sqs_1.ReceiveMessageCommand({
            QueueUrl: process.env.SQS_QUEUE_URL, // SQS Queue URL from .env
            MaxNumberOfMessages: 1, // Receive one message at a time
            WaitTimeSeconds: 20, // Wait time for long polling
        });
        // Infinite loop to continuously poll for messages
        while (true) {
            const { Messages } = yield client.send(command); // Receive messages from SQS
            if (!Messages) {
                console.log("No message in queue");
                continue; // Continue polling if no message is found
            }
            try {
                for (const message of Messages) {
                    const { MessageId, Body } = message;
                    console.log("Message received", { MessageId, Body });
                    if (!Body)
                        continue; // Skip if the message body is empty
                    // Parse the S3 event from the message body
                    const event = JSON.parse(Body);
                    // Ignore test events from S3
                    if ("Service" in event && "Event" in event) {
                        if (event.Event === "s3.TestEvent") {
                            // Delete the test message from the queue
                            yield client.send(new client_sqs_1.DeleteMessageCommand({
                                QueueUrl: process.env.SQS_QUEUE_URL, // SQS Queue URL from .env
                                ReceiptHandle: message.ReceiptHandle, // Handle to delete the specific message
                            }));
                            continue; // Skip to the next message
                        }
                    }
                    // Process the actual S3 event
                    for (const record of event.Records) {
                        const { eventName, s3 } = record;
                        const { bucket, object: { key }, } = s3;
                        console.log(s3, eventName);
                        // Run an ECS task to process the video with the received S3 key
                        const runCommand = new client_ecs_1.RunTaskCommand({
                            taskDefinition: process.env.ECS_TASK_DEFINITION, // Task definition ARN from .env
                            cluster: process.env.ECS_CLUSTER_ARN, // ECS cluster ARN from .env
                            launchType: "FARGATE", // ECS launch type
                            networkConfiguration: {
                                awsvpcConfiguration: {
                                    assignPublicIp: "ENABLED", // Enable public IP for the task
                                    securityGroups: [process.env.ECS_SECURITY_GROUP], // Security group ID from .env
                                    subnets: [
                                        process.env.ECS_SUBNET_1, // Subnet IDs from .env
                                        process.env.ECS_SUBNET_2,
                                        process.env.ECS_SUBNET_3,
                                    ],
                                },
                            },
                            overrides: {
                                containerOverrides: [
                                    {
                                        name: "video-transcoder", // Container name inside the task
                                        environment: [
                                            { name: "BUCKET_NAME", value: bucket.name }, // Pass bucket name as environment variable
                                            { name: "KEY", value: key }, // Pass S3 object key as environment variable
                                        ],
                                    },
                                ],
                            },
                        });
                        // Send the ECS command to run the task
                        yield ecsClient.send(runCommand);
                        console.log("Docker ran successfully");
                        // Delete the processed message from the queue
                        yield client.send(new client_sqs_1.DeleteMessageCommand({
                            QueueUrl: process.env.SQS_QUEUE_URL, // SQS Queue URL from .env
                            ReceiptHandle: message.ReceiptHandle, // Handle to delete the message
                        }));
                        console.log("Message deleted from queue");
                    }
                }
            }
            catch (error) {
                console.log("Error processing message", error);
            }
        }
    });
}
// Start the process
init();
