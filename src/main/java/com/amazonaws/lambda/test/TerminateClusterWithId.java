package com.amazonaws.lambda.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.TerminateJobFlowsRequest;

public class TerminateClusterWithId 
{
	private static final Log LOG = LogFactory.getLog(TerminateClusterWithId.class);	
	public TerminateClusterWithId()
	{

	}

	public String terminateEMRCluster(String clusterId) throws InterruptedException
	{

		String resposne = null;

		//AWSCredentials credentials = new BasicAWSCredentials(accessKey, secerectKey);
		AmazonElasticMapReduceClient emr = (AmazonElasticMapReduceClient) AmazonElasticMapReduceClientBuilder.standard()
				//.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion("eu-west-1")
				.build();
        //to check running state of EMR
		resposne = toTerminateCluster(emr,clusterId);
		if(null!=resposne)
		{
		//it will terminate if it is in wating state
		TerminateJobFlowsRequest terminate= new TerminateJobFlowsRequest().withJobFlowIds(clusterId);
		emr.terminateJobFlows(terminate);
		}
		//resposne = true;




		return resposne;

	}
	public static String describeClusterState(AmazonElasticMapReduce client, String clusterId) 
	{
		DescribeClusterRequest describeClusterRequest = new DescribeClusterRequest().withClusterId(clusterId);
		DescribeClusterResult result = client.describeCluster(describeClusterRequest);
		
		if (result != null) 
		{

			return result.getCluster().getStatus().getStateChangeReason().getMessage();
			
		}
		return null;
	}

	public String toTerminateCluster(AmazonElasticMapReduce client, String clusterId) throws InterruptedException 
	{
		String response = null;
		
		while (true) 
		{
      	//System.out.println(" Starting 70 seconds sleep");
			
			Thread.sleep(70000l);
			
			//System.out.println(" End of  70 seconds sleep");
			String mesg = describeClusterState(client, clusterId);
			LOG.info("State  :   " + mesg);

			if ((mesg.contains(System.getenv("Step_Completion")) || mesg.contains(System.getenv("Step_Failed")))) 
			{
				response = mesg;
				break;

			}
			//System.out.println("no of time it get executed");
			continue;
		}
		return response;
		
	}


}






