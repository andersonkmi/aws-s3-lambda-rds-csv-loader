package org.sharpsw;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.sharpsw.data.DataLoader;
import org.sharpsw.data.DatabaseConnectionBuilder;
import org.sharpsw.data.ItemDataLoader;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class AwsLambdaFunctionHandler implements RequestHandler<S3Event, Report> {

    public Report handleRequest(S3Event s3event, Context context) {

        long startTime = System.currentTimeMillis();
        Report statusReport = new Report();
        LambdaLogger logger = context.getLogger();

        logger.log("Lambda Function Started");
        Connection connection = null;

        try {
            InputStream is = AwsLambdaFunctionHandler.class.getResourceAsStream("/lambda.properties");
            Properties properties = new Properties();
            properties.load(is);

            S3EventNotification.S3EventNotificationRecord record = s3event.getRecords().get(0);
            String srcBucket = record.getS3().getBucket().getName();
            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getKey().replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");

            AmazonS3 client = AmazonS3ClientBuilder.standard().build();
            S3Object s3Object = client.getObject(new GetObjectRequest(srcBucket, srcKey));
            statusReport.setFileSize(s3Object.getObjectMetadata().getContentLength());

            logger.log("S3 Event Received: " + srcBucket + "/" + srcKey);
            DataLoader loader = null;
            if(srcKey.contains("item.csv")) {
                loader = new ItemDataLoader();
            } else if(srcKey.contains("fabricante.csv")) {
                statusReport.setExecutiongTime(System.currentTimeMillis() - startTime);
                return statusReport;
            }

            InputStreamReader isr = new InputStreamReader(s3Object.getObjectContent());
            BufferedReader br = new BufferedReader(isr);

            DatabaseConnectionBuilder builder = new DatabaseConnectionBuilder();
            connection = builder.getConnection(properties.getProperty("DB_SERVER"), properties.getProperty("DB_USER"), properties.getProperty("DB_PASSWORD"), properties.getProperty("DB_NAME"));

            int lineNum = 0;
            String line = null;
            while ((line = br.readLine()) != null) {
                if(lineNum > 0) {
                    loader.load(connection, line);
                }
                lineNum++;
            }
        } catch (Exception exception) {
            logger.log(exception.getMessage());
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException exception) {
                    logger.log(exception.getMessage());
                }
            }
        }

        statusReport.setExecutiongTime(System.currentTimeMillis() - startTime);
        return statusReport;
    }
}
