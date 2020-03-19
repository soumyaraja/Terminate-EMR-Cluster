package com.amazonaws.lambda.test;

import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3LowLevel {
	private static final Log LOG = LogFactory.getLog(S3LowLevel.class);
	public String  deleteClusterId(String bucketName, String key) throws IOException {
		String clientRegion = "eu-west-1";

		
		//AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				//.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withRegion(clientRegion)
				.build();
		
		String id = null;

		// true to create object

		/*if (s3Client.listObjects(bucketName, key).getObjectSummaries().isEmpty()) 
		{
			System.out.println("if Block");
			s3Client.putObject(bucketName, key + objectName, objectName);
			id = "done";
		}

		// false to delete object
		else
		{*/
		if (!(s3Client.listObjects(bucketName, key).getObjectSummaries().isEmpty()))
		{
			LOG.info("Start Deleting ");
			ListObjectsV2Request req = new  ListObjectsV2Request().withBucketName(bucketName).withPrefix(key);
			ListObjectsV2Result listing = s3Client.listObjectsV2(req);
			//sort s3 object with last modified date
			listing.getObjectSummaries().sort(Comparator.comparing(S3ObjectSummary::getLastModified));
			for(S3ObjectSummary summary: listing.getObjectSummaries()) 
			{
                
				
				LOG.info(summary.getKey() + summary.getLastModified()); 
				 id =summary.getKey().replace(key, ""); 
				 
				//s3Client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(summary.getKey()));
				 s3Client.deleteObject(bucketName, summary.getKey());
			}
			
			s3Client.shutdown();
			LOG.info("Cluster ID " +id);
			return id;
		}
          return id;
	}
	
	
	
	
}
