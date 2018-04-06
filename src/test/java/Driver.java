import com.aladdin.securities.dynamodbelasticsearch.replicator.AWSDeserializer;
import com.aladdin.securities.dynamodbelasticsearch.replicator.Replicator;
import com.aladdin.securities.dynamodbelasticsearch.replicator.ReplicatorTest;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.util.stream.Collectors.joining;

public class Driver {
    public static void main(String[] args) throws IOException {
        Replicator replicator = new Replicator();
        BufferedReader reader = new BufferedReader(new InputStreamReader(ReplicatorTest.class.getClassLoader().getResourceAsStream("dynamodb_insert_event.json")));
        String json = reader.lines().collect(joining("\n"));
        DynamodbEvent event = AWSDeserializer.deserializeDynamoEvents(json);
        replicator.handle(event, null);
    }
}
