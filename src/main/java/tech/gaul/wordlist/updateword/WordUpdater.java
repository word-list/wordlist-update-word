package tech.gaul.wordlist.updateword;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

import lombok.Builder;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import tech.gaul.wordlist.updateword.models.Word;

@Builder
@Getter
public class WordUpdater {
    
    private LambdaLogger logger;
    private DynamoDbEnhancedClient dbClient;
    
    public void updateWords(Iterable<Word> words) {

        // TODO: Limit to 25 words at a time (DynamoDB BatchWriteItem limit).

        WriteBatch.Builder<Word> batchBuilder = WriteBatch.builder(Word.class);

        words.forEach(batchBuilder::addPutItem);

        dbClient.batchWriteItem(b -> b
            .writeBatches(batchBuilder.build()));

    }

}
