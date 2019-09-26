package com.erfangc.dynamodb.elasticsearch;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.erfangc.dynamodb.elasticsearch.converter.JacksonConverterException;
import com.erfangc.dynamodb.elasticsearch.converter.JacksonConverterImpl;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
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
import java.util.List;
import java.util.Map;

import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.joining;

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

    public enum EventType {
        REMOVE, MODIFY, INSERT
    }

    /**
     * the function to handle {@link DynamodbEvent} from a DynamoDB Stream
     *
     * @param event   the event object
     * @param context the context object
     */
    public void handle(DynamodbEvent event, Context context) {
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
                String id = streamRecord.getKeys().entrySet().stream().map(entry -> entry.getValue().getS()).collect(joining(":"));

                if (EventType.valueOf(eventName) == EventType.INSERT || EventType.valueOf(eventName) == EventType.MODIFY) {
                    final Map<String, AttributeValue> newImage = streamRecord.getNewImage();
                    if (newImage == null) {
                        throw new RuntimeException("NewImage cannot be null, sequenceNumber:" + streamRecord.getSequenceNumber());
                    }
                    JsonNode payload = converter.mapToJsonObject(newImage);
                    final IndexRequest indexRequest = new IndexRequest(INDEX, "_doc", id).source(payload.toString(), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                    System.out.println("IndexRequest:" + indexRequest.toString());
                } else if (EventType.valueOf(eventName) == EventType.REMOVE) {
                    final DeleteRequest deleteRequest = new DeleteRequest(INDEX, "_doc", id);
                    bulkRequest.add(deleteRequest);
                    System.out.println("IndexRequest:" + deleteRequest.toString());
                }
            } catch (JacksonConverterException e) {
                e.printStackTrace();
            }
        }
        executeElasticsearchRESTRequest(bulkRequest);
    }

    private void executeElasticsearchRESTRequest(BulkRequest request) {
        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USERNAME, PASSWORD));
        RestClientBuilder restClient = RestClient
                .builder(new HttpHost(HOST, parseInt(PORT), SCHEME))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        RestHighLevelClient client = new RestHighLevelClient(restClient);
        try {
            BulkResponse responses = client.bulk(request, RequestOptions.DEFAULT);
            for (BulkItemResponse bulkItemResponse : responses.getItems()) {
                System.out.println(bulkItemResponse.getOpType() + " id: " + bulkItemResponse.getItemId() + " status:" + bulkItemResponse.status());
                if (bulkItemResponse.status() == RestStatus.BAD_REQUEST) {
                    System.err.println(bulkItemResponse.getFailureMessage());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
