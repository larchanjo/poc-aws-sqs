package com.example.aws.sqs;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StandardConsumer {

  private final static AmazonSQS sqs = AmazonSQSClientBuilder.standard()
      .withRegion(Regions.US_WEST_2)
      .withCredentials(new EnvironmentVariableCredentialsProvider())
      .build();

  public static void main(String[] args) throws InterruptedException {
    GetQueueUrlResult queueUrlResult = sqs.getQueueUrl("standard-queue");

    while (true) {
      ReceiveMessageResult messageResult = sqs.receiveMessage(queueUrlResult.getQueueUrl());
      List<Message> messages = messageResult.getMessages();
      messages.forEach(message -> {
        System.out.println(String.format("Received=[%s]", message.getBody()));
        sqs.deleteMessage(queueUrlResult.getQueueUrl(), message.getReceiptHandle());
      });

      TimeUnit.MILLISECONDS.sleep(200);
    }
  }

}
