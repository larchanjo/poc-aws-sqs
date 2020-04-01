package com.example.aws.sqs;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.QueueNameExistsException;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FifoProducer {

  private final static AtomicInteger counter = new AtomicInteger(1);

  private final static AmazonSQS sqs = AmazonSQSClientBuilder.standard()
      .withRegion(Regions.US_WEST_2)
      .withCredentials(new EnvironmentVariableCredentialsProvider())
      .build();

  public static void main(String[] args) throws InterruptedException {
    String queue = createQueue();

    while (true) {
      SendMessageRequest message = new SendMessageRequest()
          .withQueueUrl(queue)
          .withMessageBody("Message " + counter.getAndIncrement())
          .withMessageGroupId("1");

      SendMessageResult response = sqs.sendMessage(message);
      System.out.println(response);

      TimeUnit.SECONDS.sleep(1);
    }

  }

  private static String createQueue() {
    String queueName = "queue.fifo";
    try {
      System.out.println(String.format("Creating queue=[%s]", queueName));

      Map<String, String> attributes = Map.of("FifoQueue", "true",
          "ContentBasedDeduplication", "true");

      CreateQueueRequest createQueueRequest = new CreateQueueRequest()
          .withQueueName(queueName)
          .withAttributes(attributes);

      return sqs.createQueue(createQueueRequest).getQueueUrl();
    } catch (QueueNameExistsException queueNameExistsException) {
      return sqs.getQueueUrl(queueName).getQueueUrl();
    }
  }

}
