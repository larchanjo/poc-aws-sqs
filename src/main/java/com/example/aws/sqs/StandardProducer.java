package com.example.aws.sqs;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class StandardProducer {

  private final static AtomicInteger counter = new AtomicInteger(1);

  private final static AmazonSQS sqs = AmazonSQSClientBuilder.standard()
      .withRegion(Regions.US_WEST_2)
      .withCredentials(new EnvironmentVariableCredentialsProvider())
      .build();

  public static void main(String[] args) throws InterruptedException {
    CreateQueueResult queue = createQueue();

    while (true) {
      SendMessageRequest message = new SendMessageRequest()
          .withQueueUrl(queue.getQueueUrl())
          .withMessageBody("Message " + counter.getAndIncrement())
          .withDelaySeconds(1);

      SendMessageResult response = sqs.sendMessage(message);
      System.out.println(response);

      TimeUnit.SECONDS.sleep(1);
    }

  }

  private static CreateQueueResult createQueue() {
    String queueName = "standard-queue";
    System.out.println(String.format("Creating queue=[%s]", queueName));

    return sqs.createQueue(queueName);
  }

}
