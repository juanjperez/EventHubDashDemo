angular.module('sbApp', [])
.controller('sbAppController', function ($scope, $timeout) {

    var queueClient = new QueueClient(
    {
        'name': metadataQueueName,
        'namespace': eventHubNamespace,
        'sasKey': sasKey,
        'sasKeyName': sasKeyName,
        'timeOut': 55,
    });

    var controlQueue = new QueueClient(
        {
            'name': controlChannelQueueName,
            'namespace': eventHubNamespace,
            'sasKey': sasKey,
            'sasKeyName': sasKeyName,
            'timeOut': 55,
        });

    var i = null;
    var maxconsumer = 20;

    $scope.knownconsumers = [];
    $scope.consumers = [];
    $scope.throughput = 0;

    $scope.senders = [];
    $scope.sendthroughput = 0;

    var lastfilled = -1;

    // Build out consumer instances and partitions
    var it = 0;
    var itout = 0;
    
    $scope.updatePartition = function(consumers, targetConsumerId, partitionId, msgpsec) 
    {
        for (var it2 = 0; it2 < consumers.length; it2++)
        {
            var recpartitions = consumers[it2].recpartitions;

            // Update the rate for that partition
            for (it = 0; it < 32; it++) {
                if ((it2 == targetConsumerId) &&                       
                    (recpartitions[it].id == partitionId))
                {
                    recpartitions[it].rate = Math.round(msgpsec);
                }

                // Clear the partition for all other consumers
                if ((it2 != targetConsumerId) &&                       
                  (recpartitions[it].id == partitionId))
                {
                    recpartitions[it].rate = 0;
                }
            }
        }
        
    }

    $scope.clearOtherConsumers = function (allconsumers, receiverId, partitionId, msgpsec) {
        for(it = 0; it < allconsumers.length; it++)
        {
            if (allconsumers[it].consumerId != receiverId)
            {
                $scope.updatePartition($scope.consumers[itout].recpartitions, partitionId, 0);
            }
        }
    }

    $scope.calculatetotalthroughput = function()
    {
        $scope.throughput = 0;
        for (itout = 0; itout < $scope.consumers.length; itout++) {
            $scope.throughput += $scope.consumers[itout].rate;
        }
    }

    $scope.sendMessageToWorker = function(machineName, message)
    {
        var qcWorker = new QueueClient(
        {
            'name': machineName,
            'namespace': eventHubNamespace,
            'sasKey': sasKey,
            'sasKeyName': sasKeyName,
            'timeOut': 0,
        });

        var msgBody = { "message": "CommandChannel", "id": 1234 };
        var props = message;

        var msg = new BrokeredMessage(msgBody, props);

        qcWorker.sendMessage(msg, function (messagingResult) {
            console.log("Done: " + messagingResult.result);
        });    
    }

    $scope.ToggleStateOfWorker = function (machineName) {
        $scope.sendMessageToWorker(machineName, [{ 'Name': 'command', 'Value': 'ToggleEPH' }]);

        // Disable the button for this machine
        var obj = $scope.findConsumer(machineName);
        if (obj != null)
        {
            obj.disabled = true;
        }
    }

    $scope.findConsumer = function(machineName)
    {
        var retObj = null;
        for (var mi = 0; mi < $scope.consumers.length; mi++) {
            if ($scope.consumers[mi].consumerId == machineName) {
                retObj = $scope.consumers[mi];
                break;
            }
        }

        return retObj;
    }

    $scope.setupConsumer = function(machineName) {
        // If we haven't already added this worker machine, add it
        var found = false;
        for (var mi = 0; mi < $scope.knownconsumers.length; mi++) {
            if ($scope.knownconsumers[mi] == machineName) {
                found = true;
            }
        }
        if (!found) {
            $scope.knownconsumers.push(machineName);
            // Add the worker and send the ack
            var newItem = { "consumerId": machineName, "recpartitions": [], "lastn": [], "rate": 0, "awake": false, "state": "Start", "disabled": false };
            for (it = 0; it < 32; it++) {
                newItem.recpartitions.push({ "id": it, "rate": 0 });
            }
            $scope.consumers.push(newItem);
        }
    }

    $scope.processRecords = function() {
        queueClient.receiveMessage(function (messagingResult) {
            console.log("In ReceiveMessage");
            if (messagingResult.httpStatusCode != 200) {
                console.log("Timeout or error: " + messagingResult.httpStatusCode);
            }
            else {
                if (messagingResult.brokeredMessage.properties["sendunit"] != null) {
                    var obj = $.parseJSON($.parseJSON(messagingResult.brokeredMessage.properties["sendunit"]));
                    var StartTime = new Date(obj.StartTime);
                    var EndTime = new Date(obj.EndTime);
                    var numberOfItems = obj.numberOfItems;
                    var diff = Math.abs(EndTime - StartTime);
                    var msgpsec = Math.round(numberOfItems / diff * 1000);
                    var senderId = obj.senderId.trim().toUpperCase();
                    console.log("SEND --- StartTime: " + obj.StartTime + " EndTime: " + obj.EndTime + " #: " + obj.numberOfItems + " Diff: " + Math.abs(EndTime - StartTime));
                    console.log("Messages/Sec = " + msgpsec);

                    // Update the senders array
                    var found = false;

                    for (itout = 0; itout < $scope.senders.length; itout++) {
                        if ($scope.senders[itout].senderId == senderId) {
                            $scope.senders[itout].rate = msgpsec;
                            found = true;
                        }
                    }

                    if (!found) {
                        // Add a new object
                        var newObj = { 'senderId': senderId, 'rate': msgpsec };
                        $scope.senders.push(newObj);
                    }

                    $scope.sendthroughput = 0;
                    for (itout = 0; itout < $scope.senders.length; itout++) {
                        $scope.sendthroughput += $scope.senders[itout].rate;
                    }

                }

                if (messagingResult.brokeredMessage.properties["recunit"] != null) {
                    var obj = $.parseJSON($.parseJSON(messagingResult.brokeredMessage.properties["recunit"]));
                    var StartTime = new Date(obj.StartTime);
                    var EndTime = new Date(obj.EndTime);
                    var numberOfItems = obj.numberOfItems;
                    var diff = Math.abs(EndTime - StartTime);
                    var msgpsec = numberOfItems / diff * 1000;
                    var partitionId = obj.PartitionId;
                    var receiverId = obj.receiverId.trim().toUpperCase();
                    //console.log("RECV --- StartTime: " + obj.StartTime + " EndTime: " + obj.EndTime + " #: " + obj.numberOfItems + " Diff: " + Math.abs(EndTime - StartTime));
                    console.log("Partition: " + partitionId + "Messages/Sec = " + msgpsec);

                    $scope.setupConsumer(receiverId);

                    // Clean average non-active consumers
                    for (itout = 0; itout < $scope.consumers.length; itout++) {
                        if ($scope.consumers[itout].state == "Start") {
                            $scope.consumers[itout].rate = 0;
                        }
                    }

                    // We fill the consumer instances as we receive them
                    for (itout = 0; itout < maxconsumer; itout++) {
                        if ($scope.consumers[itout].consumerId == receiverId) {
                            $scope.updatePartition($scope.consumers, itout, partitionId, msgpsec);

                            // Add this message to the lastn for this consumer
                            var lengthoflastn = $scope.consumers[itout].lastn.length;
                            $scope.consumers[itout].lastn.push(msgpsec);

                            if (lengthoflastn > 5) {
                                // Remove the last one
                                $scope.consumers[itout].lastn.shift();
                            }

                            // Calculate Average of lastn
                            var sum = 0;
                            for (var it3 = 0; it3 < lengthoflastn; it3++) {
                                var currentItem = $scope.consumers[itout].lastn[it3];
                                sum += currentItem;
                            }
                            // Save the average
                            $scope.consumers[itout].rate = Math.round(sum / lengthoflastn);


                            $scope.calculatetotalthroughput();

                            break;
                        }

                        if (itout > lastfilled) {
                            // Add the new receiverId
                            $scope.consumers[itout].consumerId = receiverId;

                            $scope.updatePartition($scope.consumers, itout, partitionId, msgpsec);

                            lastfilled = itout;
                            $scope.consumers[itout].lastn.push(msgpsec);
                            break;
                        }
                    }
                }
            }
            $timeout($scope.processRecords, 0);
        });
    }

    $scope.processControlMessages = function() {
        controlQueue.receiveMessage(function (messagingResult) {
            console.log("In Control ReceiveMessage");
            if (messagingResult.httpStatusCode != 200) {
                console.log("Timeout or error: " + messagingResult.httpStatusCode);
            }
            else {

                if (messagingResult.brokeredMessage.properties["controlrec"] != null) {
                    console.log("In");
                    var machineName = $.parseJSON(messagingResult.brokeredMessage.properties["controlrec"]).trim().toUpperCase();
                    var msg = $.parseJSON(messagingResult.brokeredMessage.properties["controlmsg"]);

                    if (msg == "STARTED" || msg == "STOPPED") {
                        $scope.setupConsumer(machineName);
                    }

                    if (msg == "STARTED") {
                        // Enable Set the machine button to 'Stop'
                        var obj = $scope.findConsumer(machineName);
                        if (obj != null) {
                            obj.state = "Stop";
                            obj.disabled = false;
                        }
                    }

                    if (msg == "STOPPED") {
                        // Enable Set the machine button to 'Start'       
                        var obj = $scope.findConsumer(machineName);
                        if (obj != null) {
                            obj.state = "Start";
                            obj.disabled = false;
                        }
                    }

                }
            }
            $timeout($scope.processControlMessages, 0);
        });
    }

    $scope.f = function () {
        console.log("Checking");
        
        $scope.processRecords();
        $scope.processControlMessages();
    }

    $timeout($scope.f, 100);

});