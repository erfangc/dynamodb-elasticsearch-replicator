package com.erfangc.dynamodb.elasticsearch;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.erfangc.dynamodb.elasticsearch.converter.JacksonConverterException;
import com.erfangc.dynamodb.elasticsearch.converter.JacksonConverterImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * {@link Replicator} is a lambda class that handles DynamoDB events from a DynamoDB Stream
 * <p>
 * We take records on the Stream and transform the record image into a JSON, which is then indexed as a nested document
 * in a Elasticsearch collection with the end point specified by the ES_* environment parameter
 * <p>
 * REMOVE events are handled as DELETE requests against the ES cluster. The goal is to keep the two databases in sync, so that
 * data in DynamoDB becomes searchable
 */
public class Replicator {

    private static final String HOST = System.getenv("ES_HOST");
    private static final String PORT = System.getenv("ES_PORT");
    private static final String SCHEME = System.getenv("ES_SCHEME");
    private static final String USERNAME = System.getenv("ES_USERNAME");
    private static final String PASSWORD = System.getenv("ES_PASSWORD");
    private static final String INDEX = System.getenv("ES_INDEX");
    private static final String dlqUrl = System.getenv("DLQ_URL");
    private static final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    private final RestHighLevelClient client;
    private final AmazonSQS sqs;

    public Replicator() {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USERNAME, PASSWORD));
        RestClientBuilder restClient = RestClient
                .builder(new HttpHost(HOST, parseInt(PORT), SCHEME))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        client = new RestHighLevelClient(restClient);
        sqs = AmazonSQSClientBuilder.defaultClient();
        List<String> missingEnvVars = Stream.of(
                "ES_HOST",
                "ES_PORT",
                "ES_SCHEME",
                "ES_USERNAME",
                "ES_PASSWORD",
                "ES_INDEX",
                "DLQ_URL"
        )
                .filter(envvar -> System.getenv(envvar) == null)
                .collect(toList());
        if (!missingEnvVars.isEmpty()) {
            System.out.println("Missing environment variables: " + missingEnvVars.toString());
            System.exit(1);
        }
    }

    public enum EventType {
        REMOVE, MODIFY, INSERT
    }

    /**
     * the function to handle {@link DynamodbEvent} from a DynamoDB Stream
     *
     * @param event   the event object
     * @param context the context object
     */
    public void handle(DynamodbEvent event, Context context) throws IOException {
        List<DynamodbEvent.DynamodbStreamRecord> records = event.getRecords();
        BulkRequest bulkRequest = new BulkRequest();
        for (DynamodbEvent.DynamodbStreamRecord record : records) {
            try {
                final JacksonConverterImpl converter = new JacksonConverterImpl();
                final String eventName = record.getEventName();
                final StreamRecord streamRecord = record.getDynamodb();
                /*
                create a primary id from record
                 */
                System.out.println(streamRecord);
                String id = getId(streamRecord);
                if (EventType.valueOf(eventName) == EventType.INSERT || EventType.valueOf(eventName) == EventType.MODIFY) {
                    final Map<String, AttributeValue> newImage = streamRecord.getNewImage();
                    if (newImage == null) {
                        throw new RuntimeException("NewImage cannot be null, sequenceNumber:" + streamRecord.getSequenceNumber());
                    }
                    JsonNode payload = converter.mapToJsonObject(newImage);
                    final IndexRequest indexRequest = new IndexRequest(INDEX).id(id).source(payload.toString(), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                    System.out.println("IndexRequest: " + indexRequest.toString());
                } else if (EventType.valueOf(eventName) == EventType.REMOVE) {
                    final DeleteRequest deleteRequest = new DeleteRequest(INDEX).id(id);
                    bulkRequest.add(deleteRequest);
                    System.out.println("IndexRequest: " + deleteRequest.toString());
                }
            } catch (JacksonConverterException e) {
                // JSON conversion exceptions will not succeed on retry, therefore do not throw an error
                String id = getId(record.getDynamodb());
                System.err.println("Failed to process record due to serialization issues");
                System.err.println(
                        "Failure detail: id=" + id
                                + " message=" + e.getMessage()
                );
            }
        }
        System.out.println("Sending bulk request for " + bulkRequest.requests().size() + " requests to Elasticsearch");
        executeElasticsearchRESTRequest(bulkRequest);
        System.out.println("Executed bulk request for " + bulkRequest.requests().size() + " requests to Elasticsearch");
    }

    private String getId(StreamRecord streamRecord) {
        return streamRecord
                .getKeys()
                .values()
                .stream()
                .map(AttributeValue::getS)
                .collect(joining(":"));
    }

    private void executeElasticsearchRESTRequest(BulkRequest request) throws IOException {
        BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
        List<BadRequest> badRequests = new ArrayList<>();
        BulkItemResponse[] items = responses.getItems();
        for (BulkItemResponse itemResponse : items) {
            int itemId = itemResponse.getItemId();
            System.out.println(
                    "Response received for operation=" + itemResponse.getOpType()
                            + " index=" + itemResponse.getIndex()
                            + " itemId=" + itemId
                            + " status=" + itemResponse.status()
            );
            if (itemResponse.status() == RestStatus.BAD_REQUEST) {
                DocWriteRequest<?> docWriteRequest = request.requests().get(itemId);
                String source = null;
                if (docWriteRequest instanceof IndexRequest) {
                    source = ((IndexRequest) docWriteRequest).source().utf8ToString();
                }
                BadRequest badRequest = new BadRequest()
                        .setSource(source)
                        .setCause(itemResponse.getFailureMessage())
                        .setIndex(itemResponse.getIndex())
                        .setId(itemResponse.getId())
                        .setTimestamp(Instant.now().toString())
                        .setOpType(itemResponse.getOpType());
                badRequests.add(badRequest);
            }
        }
        if (!badRequests.isEmpty()) {
            System.err.println(badRequests.size() + " out of " + items.length + " requests to Elasticsearch failed");
            logBadRequests(badRequests);
        }
    }

    // 400s from Elasticsearch will not succeed on retry, therefore we do not fail the Lambda
    private void logBadRequests(List<BadRequest> badRequests) throws JsonProcessingException {
        for (BadRequest badRequest : badRequests) {
            System.err.println(
                    "Failure detail: id=" + badRequest.getId()
                            + " message=" + badRequest.getCause()
                            + " index=" + badRequest.getIndex());
            SendMessageRequest sendMessageRequest = new SendMessageRequest();
            //
            // write the failed request to a dead-letter-queue
            //
            sendMessageRequest.withQueueUrl(dlqUrl).withMessageBody(objectMapper.writeValueAsString(badRequest));
            sqs.sendMessage(sendMessageRequest);
        }
    }

}
