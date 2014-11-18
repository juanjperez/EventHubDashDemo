var metadataQueueName = "stelemetryqueue";      // Create through Azure Portal
var controlChannelQueueName = "controlqueue";   // Create through Azure Portal

var eventHubNamespace = "<the Service Bus namespace which contains your event hub and queues>";
var sasKey = "<primary key for RootManageSharedAccessKey>";
var sasKeyName = "RootManageSharedAccessKey";