package utils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;

public class S3Handler {
    private static AmazonS3 s3;
    private final String BUCKET = "bucket1586960757979w";

    public S3Handler() {
        s3 = new AmazonS3Client();
        com.amazonaws.regions.Region usEast1 = com.amazonaws.regions.Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
    }

    public void upload(File file, String key) {
        s3.putObject(new PutObjectRequest(BUCKET, key, file));
    }

    public BufferedReader download(String key) {
        S3Object object = s3.getObject(new GetObjectRequest(BUCKET, key));
        return new BufferedReader(new InputStreamReader(object.getObjectContent()));
    }
}
