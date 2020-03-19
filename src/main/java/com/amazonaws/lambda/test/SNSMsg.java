package com.amazonaws.lambda.test;




import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
public class SNSMsg 
{
private static final Log LOG = LogFactory.getLog(SNSMsg.class);	
	public SNSMsg()
	{
		
	}
	
	protected static String publishMsg()
	{  
		
		
		AmazonSNSClient snsClient =  (AmazonSNSClient) AmazonSNSClientBuilder.standard()
		.withRegion(Regions.EU_WEST_1)
		.build();
		
		 String topicArn = System.getenv("Ec2_Operation_Topic");
		 String msg = "START EC2";
		 PublishRequest publishRequest = new PublishRequest(topicArn, msg);
		 PublishResult publishResult = snsClient.publish(publishRequest);
		 //print MessageId of message published to SNS topic
		 LOG.info("MessageId - " + publishResult.getMessageId());
		 
		 return publishResult.getMessageId();
		
	}
	
	
}
