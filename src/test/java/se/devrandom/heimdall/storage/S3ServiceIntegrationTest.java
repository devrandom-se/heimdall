package se.devrandom.heimdall.storage;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class S3ServiceIntegrationTest {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String ORG_ID = "00D000000000001";
    private static final String REGION = "us-east-1";

    @Container
    private static final MinIOContainer minio = new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z")
            .withEnv("MINIO_DOMAIN", "localhost");

    private static String prevAccessKey;
    private static String prevSecretKey;
    private static String prevEndpoint;

    @BeforeAll
    static void setUpAll() {
        prevAccessKey = System.getProperty("aws.accessKeyId");
        prevSecretKey = System.getProperty("aws.secretAccessKey");
        prevEndpoint = System.getProperty("aws.endpointUrl");

        System.setProperty("aws.accessKeyId", minio.getUserName());
        System.setProperty("aws.secretAccessKey", minio.getPassword());
        System.setProperty("aws.endpointUrl", minio.getS3URL());

        // Create test bucket via raw S3Client pointed at MinIO
        try (S3Client s3Client = createRawMinioClient()) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
        }
    }

    @AfterAll
    static void tearDownAll() {
        restoreProperty("aws.accessKeyId", prevAccessKey);
        restoreProperty("aws.secretAccessKey", prevSecretKey);
        restoreProperty("aws.endpointUrl", prevEndpoint);
    }

    private static void restoreProperty(String key, String previousValue) {
        if (previousValue != null) {
            System.setProperty(key, previousValue);
        } else {
            System.clearProperty(key);
        }
    }

    private static S3Client createRawMinioClient() {
        return S3Client.builder()
                .endpointOverride(URI.create(minio.getS3URL()))
                .region(Region.of(REGION))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(minio.getUserName(), minio.getPassword())))
                .forcePathStyle(true)
                .build();
    }

    @Test
    void uploadCsvToS3_writesToMinioWhenEndpointOverridden(@TempDir Path tempDir) throws Exception {
        // Arrange: prepare a CSV file
        Path csvFile = tempDir.resolve("Account.csv");
        String csvContent = "Id,Name,IsDeleted\n001000000000001,Acme Corp,false\n";
        Files.writeString(csvFile, csvContent, StandardCharsets.UTF_8);

        // Instantiate S3Service (configured via system properties for MinIO endpoint and credentials)
        S3Service s3Service = new S3Service(BUCKET_NAME, REGION, ORG_ID);

        // Act: upload CSV via S3Service
        String s3Key = s3Service.uploadCsvToS3(csvFile, "Account", false);
        assertNotNull(s3Key);
        assertTrue(s3Key.contains("Account"));

        // Assert: independently verify the object exists and matches in MinIO
        try (S3Client rawClient = createRawMinioClient()) {
            HeadObjectResponse headResponse = rawClient.headObject(
                    HeadObjectRequest.builder().bucket(BUCKET_NAME).key(s3Key).build());
            assertNotNull(headResponse);
            assertEquals(csvContent.getBytes(StandardCharsets.UTF_8).length, headResponse.contentLength());

            String fetchedContent;
            try (var is = rawClient.getObject(GetObjectRequest.builder().bucket(BUCKET_NAME).key(s3Key).build())) {
                fetchedContent = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                        .lines()
                        .collect(Collectors.joining("\n", "", "\n"));
            }
            assertEquals(csvContent, fetchedContent);
        } finally {
            s3Service.close();
        }
    }
}
