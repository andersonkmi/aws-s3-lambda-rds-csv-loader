package org.sharpsw;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.net.URLDecoder;

public class AwsLambdaFunctionHandler implements RequestHandler<S3Event, Report> {

    public Report handleRequest(S3Event s3event, Context context) {

        long startTime = System.currentTimeMillis();
        Report statusReport = new Report();
        LambdaLogger logger = context.getLogger();

        logger.log("Lambda Function Started");
        //Helper helper = new Helper();

        try {
            S3EventNotification.S3EventNotificationRecord record = s3event.getRecords().get(0);
            String srcBucket = record.getS3().getBucket().getName();
            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getKey().replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");

            AmazonS3Client client = new AmazonS3Client();
            S3Object s3Object = client.getObject(new GetObjectRequest(srcBucket, srcKey));
            statusReport.setFileSize(s3Object.getObjectMetadata().getContentLength());

            logger.log("S3 Event Received: " + srcBucket + "/" + srcKey);

        } catch (Exception exception) {
            logger.log(exception.getMessage());
        }

        statusReport.setExecutiongTime(System.currentTimeMillis() - startTime);
        return statusReport;
    }
}
