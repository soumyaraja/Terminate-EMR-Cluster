package com.amazonaws.lambda.test;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;


public class LambdaFunctionHandler implements RequestHandler<SNSEvent, String> {
private static final Log LOG = LogFactory.getLog(LambdaFunctionHandler.class);

	private static String bucketName = System.getenv("HadoopJarStepConfig_withBucketName");
	private static String key = "clusterId/";


	public LambdaFunctionHandler() 
	{

	}

	// Test purpose only.

	@Override
	public String handleRequest(SNSEvent event, Context context) {

		String clusterId = null;
		String message = event.getRecords().get(0).getSNS().getMessage();
		LOG.info(message);
		context.getLogger().log("From SNS Lambda Termination: " + message);
		String resposne =null;  

		//working cluster termination code
		//get cluster id from S3

		S3LowLevel s3Client = new S3LowLevel();
		try {
			clusterId =  s3Client.deleteClusterId(bucketName, key);
			if(null!=clusterId)
			{
				LOG.info("  ClusterId from S3   "  + clusterId); 
				//Terminate with clusterId received from S3
				TerminateClusterWithId terClustewid = new TerminateClusterWithId();

				try {
					resposne = terClustewid.terminateEMRCluster(clusterId);
					if(resposne.contains(System.getenv("Step_Completion")))
						SNSMsg.publishMsg();

				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				LOG.info("  Termination Completed   "  + resposne);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//end of cluster termination code

		return message;

	}
}