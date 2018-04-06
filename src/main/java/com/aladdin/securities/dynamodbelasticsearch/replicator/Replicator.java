package com.aladdin.securities.dynamodbelasticsearch.replicator;

import com.aladdin.securities.dynamodbelasticsearch.replicator.converter.JacksonConverterException;
import com.aladdin.securities.dynamodbelasticsearch.replicator.converter.JacksonConverterImpl;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.StreamRecord;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

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
    private static final String AUTHORIZATION = System.getenv("ES_AUTHORIZATION");
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
                    System.out.println("IndexRequest:"+ indexRequest.toString());
                } else if (EventType.valueOf(eventName) == EventType.REMOVE) {
                    final DeleteRequest deleteRequest = new DeleteRequest(INDEX, "_doc", id);
                    bulkRequest.add(deleteRequest);
                    System.out.println("IndexRequest:"+ deleteRequest.toString());
                }
            } catch (JacksonConverterException e) {
                e.printStackTrace();
            }
        }
        executeElasticsearchRESTRequest(bulkRequest);
    }

    private void executeElasticsearchRESTRequest(BulkRequest request) {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient
                        .builder(new HttpHost(HOST, parseInt(PORT), SCHEME))
                        .setDefaultHeaders(new Header[]{new BasicHeader(HttpHeaders.AUTHORIZATION, AUTHORIZATION)})
        );
        try {
            BulkResponse responses = client.bulk(request);
            for (BulkItemResponse bulkItemResponse : responses.getItems()) {
                System.out.println(bulkItemResponse.getOpType() + " id: " + bulkItemResponse.getItemId() + " status:" + bulkItemResponse.status());
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
