package examples.sparkstreaming;/*
 * Copyright 2012-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

public class KinesisStreamProducer {

    static AmazonKinesisClient kinesisClient;
    private static final Log LOG = LogFactory.getLog(KinesisStreamProducer.class);

    private static void init() throws Exception {
        AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();;
        kinesisClient = new AmazonKinesisClient(credentials);
    }

    public static void main(String[] args) throws Exception {
        init();

        final String myStreamName = "WordStream";
        final Integer myStreamSize = 1;

        // list all of my streams
        ListStreamsRequest listStreamsRequest = new ListStreamsRequest();
        listStreamsRequest.setLimit(10);
        ListStreamsResult listStreamsResult = kinesisClient.listStreams(listStreamsRequest);
        List<String> streamNames = listStreamsResult.getStreamNames();
        while (listStreamsResult.isHasMoreStreams()) {
            if (streamNames.size() > 0) {
                listStreamsRequest.setExclusiveStartStreamName(streamNames.get(streamNames.size() - 1));
            }

            listStreamsResult = kinesisClient.listStreams(listStreamsRequest);
            streamNames.addAll(listStreamsResult.getStreamNames());
        }
        LOG.info("Printing my list of streams : ");

        // print all of my streams.
        if (!streamNames.isEmpty()) {
            System.out.println("List of my streams: ");
        }
        for (int i = 0; i < streamNames.size(); i++) {
            System.out.println(streamNames.get(i));
        }

        LOG.info("Putting records in stream : " + myStreamName);

        while (true) {
            PutRecordRequest putRecordRequest = new PutRecordRequest();
            putRecordRequest.setStreamName(myStreamName);
            putRecordRequest.setData(ByteBuffer.wrap("This is some arbitrary text coming through a Kinesis shard".getBytes()));
            putRecordRequest.setPartitionKey(String.format("partitionKey-%d", 99));
            PutRecordResult putRecordResult = kinesisClient.putRecord(putRecordRequest);
            System.out.println("Successfully putrecord, partition key : "
                    + putRecordRequest.getPartitionKey()
                    + ", ShardID : " + putRecordResult.getShardId());
            Thread.sleep(2000);
        }
    }
}