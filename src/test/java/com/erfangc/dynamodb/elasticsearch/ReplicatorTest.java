package com.erfangc.dynamodb.elasticsearch;

import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.junit.Assert.*;

public class ReplicatorTest {

    @Test
    public void handle() throws IOException {
        // TODO figure out how to test lambda functions
//        Replicator replicator = new Replicator();
//        BufferedReader reader = new BufferedReader(new InputStreamReader(ReplicatorTest.class.getClassLoader().getResourceAsStream("dynamodb_event.json")));
//        String json = reader.lines().collect(joining("\n"));
//        DynamodbEvent event = AWSDeserializer.deserializeDynamoEvents(json);
//        replicator.handle(event, null);
    }
}