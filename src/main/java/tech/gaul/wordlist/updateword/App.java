package tech.gaul.wordlist.updateword;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import tech.gaul.wordlist.updateword.models.UpdateWordMessage;
import tech.gaul.wordlist.updateword.models.Word;

public class App implements RequestHandler<SQSEvent, Object> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DynamoDbEnhancedClient dbClient = DependencyFactory.dynamoDbClient();        
    private final Optional<UpdateWordMessage> emptyRequest = Optional.empty();

    @Override
    public Object handleRequest(SQSEvent event, Context context) {

        List<Word> words = event.getRecords().stream()
                .map(SQSEvent.SQSMessage::getBody)
                .map(str -> {
                    try {
                        return Optional.of(objectMapper.readValue(str, UpdateWordMessage.class));
                    } catch (Exception e) {
                        return emptyRequest;
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(message -> Word.builder()
                        .word(message.getWord())
                        .types(message.getTypes())
                        .commonness(message.getCommonness())
                        .offensiveness(message.getOffensiveness())
                        .sentiment(message.getSentiment())
                        .build())
                .collect(Collectors.toList());

        WordUpdater updater = WordUpdater.builder()
                .logger(context.getLogger())
                .dbClient(dbClient)
                .build();

        updater.updateWords(words);

        return null;
    }
}