package tech.gaul.wordlist.updateword;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

import lombok.Builder;
import lombok.Getter;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteResult;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;
import tech.gaul.wordlist.updateword.models.Word;

@Builder
@Getter
public class WordUpdater {

    private LambdaLogger logger;
    private DynamoDbEnhancedClient dbClient;

    private final TableSchema<Word> wordTableSchema = TableSchema.fromClass(Word.class);

    // Limit to 25 words at a time (DynamoDB BatchWriteItem limit)
    private final int BATCH_SIZE = 25;

    public void updateWords(List<Word> words) {

        Map<String, Word> wordMap = words.stream().collect(Collectors.toMap(Word::getWord, Function.identity()));

        DynamoDbTable<Word> mappedTable = dbClient.table(System.getenv("WORDS_TABLE_NAME"), wordTableSchema);

        int baseDelay = 1000;
        Random random = new Random();
        int batchNumber = 1;
        int expectedBatchCount = wordMap.size() / BATCH_SIZE;

        while (!wordMap.isEmpty()) {
            List<Word> wordsToProcess = wordMap.values()
                    .stream()
                    .limit(BATCH_SIZE)
                    .collect(Collectors.toList());

            WriteBatch.Builder<Word> batchBuilder = WriteBatch.builder(Word.class)
                    .mappedTableResource(mappedTable);

            wordsToProcess.forEach(batchBuilder::addPutItem);

            BatchWriteResult result = dbClient.batchWriteItem(b -> b
                    .writeBatches(batchBuilder.build()));

            Set<String> unprocessed = result.unprocessedPutItemsForTable(mappedTable)
                    .stream()
                    .map(Word::getWord)
                    .collect(Collectors.toSet());

            wordsToProcess.stream()
                    .filter(w -> !unprocessed.contains(w.getWord()))
                    .forEach(wordsToProcess::remove);

            // Back-off timer
            int backOffTime = baseDelay * (int) Math.pow(2, batchNumber - 1);
            int jitter = random.nextInt(500); // Add up to 500ms of jitter
            backOffTime += jitter;

            try {
                Thread.sleep(backOffTime);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                System.out.println("Thread interrupted during back-off timer. Exiting.");
                break;
            }

            batchNumber++;        
            if (batchNumber > expectedBatchCount) {
                if (batchNumber % 100 == 0) {
                    logger.log(String.format("Batch number %d is greater than the expected %d total batches", batchNumber, expectedBatchCount));
                }
            }
        }
    }

}
