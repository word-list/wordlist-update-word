package tech.gaul.wordlist.updateword;

import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DependencyFactory {

    private DependencyFactory() {
    }

    public static DynamoDbEnhancedClient dynamoDbClient() {
        DynamoDbClient dbClient = DynamoDbClient.builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .region(Region.of(System.getenv(SdkSystemSetting.AWS_REGION.environmentVariable())))
                .httpClientBuilder(AwsCrtHttpClient.builder())
                .build();

        return DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dbClient)
                .build();
    }
}
