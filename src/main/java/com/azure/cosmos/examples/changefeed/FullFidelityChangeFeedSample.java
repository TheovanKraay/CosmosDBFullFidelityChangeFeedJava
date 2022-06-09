package com.azure.cosmos.examples.changefeed;

import com.azure.cosmos.implementation.guava25.collect.Multimap;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.implementation.guava25.collect.ArrayListMultimap;
import com.azure.cosmos.models.ChangeFeedPolicy;
import com.azure.cosmos.models.CosmosChangeFeedRequestOptions;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.FeedRange;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class FullFidelityChangeFeedSample {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PARTITION_KEY_FIELD_NAME = "mypk";
    public static CosmosClient client;
    public static CosmosAsyncClient clientAsync;
    private CosmosAsyncContainer createdAsyncContainer;
    private CosmosAsyncDatabase createdAsyncDatabase;
    private CosmosContainer createdContainer;
    private static CosmosDatabase createdDatabase;
    private final Multimap<String, ObjectNode> partitionKeyToDocuments = ArrayListMultimap.create();

    public static final String DATABASE_NAME = "db";
    public static final String COLLECTION_NAME = "ffcf";
    protected static Logger logger = LoggerFactory.getLogger(FullFidelityChangeFeedSample.class);

    public static void main(String[] args) {
        logger.info("BEGIN Sample");
        // FullFidelityChangeFeedSample demo = new FullFidelityChangeFeedSample();
        client = FullFidelityChangeFeedSample.getCosmosClient();
        clientAsync = FullFidelityChangeFeedSample.getCosmosAsyncClient();

        try {
            logger.info("-->RUN asyncChangeFeed_fromNow_fullFidelity_forFullRange");

            FullFidelityChangeFeedSample demo = new FullFidelityChangeFeedSample();
            demo.asyncChangeFeed_fromNow_fullFidelity_forFullRange();

            logger.info("-->DELETE sample's database: " + DATABASE_NAME);

            deleteDatabase(createdDatabase);

            Thread.sleep(500);

        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.info("END Sample");
    }

    public void asyncChangeFeed_fromNow_fullFidelity_forFullRange() throws Exception {
        this.createContainer(
                (cp) -> cp.setChangeFeedPolicy(ChangeFeedPolicy.createFullFidelityPolicy(Duration.ofMinutes(60))));
        insertDocuments(8, 15);
        updateDocuments(3, 5);
        deleteDocuments(2, 3);

        Runnable updateAction1 = () -> {
            insertDocuments(5, 9);
            updateDocuments(3, 5);
            deleteDocuments(2, 3);
        };

        Runnable updateAction2 = () -> {
            updateDocuments(5, 2);
            deleteDocuments(2, 3);
            insertDocuments(10, 5);
        };

        final int expectedInitialEventCount = 0;

        final int expectedEventCountAfterFirstSetOfUpdates = 5 * 9 // events for inserts
                + 3 * 5 // event count for updates
                + 2 * 3; // plus deletes (which are all included in FF CF)

        final int expectedEventCountAfterSecondSetOfUpdates = 10 * 5 // events for inserts
                + 5 * 2 // event count for updates
                + 2 * 3; // plus deletes (which are all included in FF CF)

        CosmosChangeFeedRequestOptions options = CosmosChangeFeedRequestOptions
                .createForProcessingFromNow(FeedRange.forFullRange());
        options.fullFidelity();

        String continuation = drainAndValidateChangeFeedResults(options, null, expectedInitialEventCount);

        // applying first set of updates
        updateAction1.run();

        options = CosmosChangeFeedRequestOptions
                .createForProcessingFromContinuation(continuation);

        continuation = drainAndValidateChangeFeedResults(
                options,
                null,
                expectedEventCountAfterFirstSetOfUpdates);

        // applying first set of updates
        updateAction2.run();

        options = CosmosChangeFeedRequestOptions
                .createForProcessingFromContinuation(continuation);

        drainAndValidateChangeFeedResults(
                options,
                null,
                expectedEventCountAfterSecondSetOfUpdates);
    }

    private String drainAndValidateChangeFeedResults(
            CosmosChangeFeedRequestOptions changeFeedRequestOptions,
            Function<CosmosChangeFeedRequestOptions, CosmosChangeFeedRequestOptions> onNewRequestOptions,
            int expectedEventCount) {

        return drainAndValidateChangeFeedResults(
                Arrays.asList(changeFeedRequestOptions),
                onNewRequestOptions,
                expectedEventCount).get(0);
    }

    private Map<Integer, String> drainAndValidateChangeFeedResults(
            List<CosmosChangeFeedRequestOptions> changeFeedRequestOptions,
            Function<CosmosChangeFeedRequestOptions, CosmosChangeFeedRequestOptions> onNewRequestOptions,
            int expectedTotalEventCount) {

        Map<Integer, String> continuations = new HashMap<>();

        int totalRetrievedEventCount = 0;

        boolean isFinished = false;
        int emptyResultCount = 0;

        while (!isFinished) {
            for (Integer i = 0; i < changeFeedRequestOptions.size(); i++) {
                List<ObjectNode> results;

                CosmosChangeFeedRequestOptions effectiveOptions;
                if (continuations.containsKey(i)) {
                    logger.info(String.format(
                            "Continuation BEFORE: %s",
                            new String(
                                    Base64.getUrlDecoder().decode(continuations.get(i)),
                                    StandardCharsets.UTF_8)));
                    effectiveOptions = CosmosChangeFeedRequestOptions
                            .createForProcessingFromContinuation(continuations.get(i));
                    if (onNewRequestOptions != null) {
                        effectiveOptions = onNewRequestOptions.apply(effectiveOptions);
                    }
                } else {
                    effectiveOptions = changeFeedRequestOptions.get(i);
                }
                effectiveOptions.fullFidelity();
                final Integer index = i;
                results = createdAsyncContainer
                        .queryChangeFeed(effectiveOptions, ObjectNode.class)
                        // NOTE - in real app you would need delaying persisting the
                        // continuation until you retrieve the next one
                        .handle((r) -> continuations.put(index, r.getContinuationToken()))
                        .collectList()
                        .block();

                logger.info(
                        String.format(
                                "Continuation AFTER: %s, records retrieved: %d",
                                new String(
                                        Base64.getUrlDecoder().decode(continuations.get(i)),
                                        StandardCharsets.UTF_8),
                                results.size()));

                totalRetrievedEventCount += results.size();
                if (totalRetrievedEventCount >= expectedTotalEventCount) {
                    isFinished = true;
                    break;
                }

                for (ObjectNode objectNode : results) {
                    logger.info("doc: " + objectNode);
                }

                if (results.size() == 0) {
                    emptyResultCount += 1;

                    // if (emptyResultCount > 10){
                    // isFinished = true;
                    // }
                    logger.info(
                            String.format("No more docs....",
                                    totalRetrievedEventCount,
                                    expectedTotalEventCount,
                                    emptyResultCount));

                    try {
                        Thread.sleep(500 / changeFeedRequestOptions.size());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    emptyResultCount = 0;
                }

            }
        }

        return continuations;
    }

    void insertDocuments(
            int partitionCount,
            int documentCount) {

        List<ObjectNode> docs = new ArrayList<>();

        for (int i = 0; i < partitionCount; i++) {
            String partitionKey = UUID.randomUUID().toString();
            for (int j = 0; j < documentCount; j++) {
                docs.add(getDocumentDefinition(partitionKey));
            }
        }

        ArrayList<Mono<CosmosItemResponse<ObjectNode>>> result = new ArrayList<>();
        for (int i = 0; i < docs.size(); i++) {
            result.add(createdAsyncContainer
                    .createItem(docs.get(i)));
        }

        List<ObjectNode> insertedDocs = Flux.merge(
                Flux.fromIterable(result),
                10)
                .map(CosmosItemResponse::getItem).collectList().block();

        for (ObjectNode doc : insertedDocs) {
            partitionKeyToDocuments.put(
                    doc.get(PARTITION_KEY_FIELD_NAME).textValue(),
                    doc);
        }
        logger.info("FINISHED INSERT");
    }

    void deleteDocuments(
            int partitionCount,
            int documentCount) {

        Collection<ObjectNode> docs;
        for (int i = 0; i < partitionCount; i++) {
            String partitionKey = this.partitionKeyToDocuments
                    .keySet()
                    .stream()
                    .skip(i)
                    .findFirst()
                    .get();

            docs = this.partitionKeyToDocuments.get(partitionKey);

            for (int j = 0; j < documentCount; j++) {
                ObjectNode docToBeDeleted = docs.stream().findFirst().get();
                createdContainer.deleteItem(docToBeDeleted, null);
                docs.remove(docToBeDeleted);
            }
        }
    }

    void updateDocuments(
            int partitionCount,
            int documentCount) {

        Collection<ObjectNode> docs;
        for (int i = 0; i < partitionCount; i++) {
            String partitionKey = this.partitionKeyToDocuments
                    .keySet()
                    .stream()
                    .skip(i)
                    .findFirst()
                    .get();

            docs = this.partitionKeyToDocuments.get(partitionKey);

            for (int j = 0; j < documentCount; j++) {
                ObjectNode docToBeUpdated = docs.stream().skip(j).findFirst().get();
                docToBeUpdated.put("someProperty", UUID.randomUUID().toString());
                createdContainer.replaceItem(
                        docToBeUpdated,
                        docToBeUpdated.get("id").textValue(),
                        new PartitionKey(docToBeUpdated.get("mypk").textValue()),
                        null);
            }
        }
    }

    private static ObjectNode getDocumentDefinition(String partitionKey) {
        String uuid = UUID.randomUUID().toString();
        String json = String.format("{ "
                + "\"id\": \"%s\", "
                + "\"mypk\": \"%s\", "
                + "\"prop\": \"%s\""
                + "}", uuid, partitionKey, uuid);

        try {
            return OBJECT_MAPPER.readValue(json, ObjectNode.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Invalid partition key value provided.");
        }
    }

    public static CosmosClient getCosmosClient() {

        return new CosmosClientBuilder()
                .endpoint(SampleConfigurations.HOST)
                .key(SampleConfigurations.MASTER_KEY)
                .contentResponseOnWriteEnabled(true)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildClient();
    }

    public static CosmosAsyncClient getCosmosAsyncClient() {

        return new CosmosClientBuilder()
                .endpoint(SampleConfigurations.HOST)
                .key(SampleConfigurations.MASTER_KEY)
                .contentResponseOnWriteEnabled(true)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildAsyncClient();
    }

    public static CosmosAsyncDatabase createNewDatabase(CosmosAsyncClient client, String databaseName) {
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName).block();
        return client.getDatabase(databaseResponse.getProperties().getId());
    }

    public static void deleteDatabase(CosmosDatabase createdDatabase2) {
        createdDatabase2.delete();
    }

    public CosmosContainerProperties createNewCollection(CosmosClient client2, String databaseName,
            String collectionName) {
        CosmosDatabaseResponse databaseResponse = client2.createDatabaseIfNotExists(databaseName);
        CosmosDatabase database = client2.getDatabase(databaseResponse.getProperties().getId());

        CosmosContainerProperties containerSettings = new CosmosContainerProperties(collectionName, "/mypk");

        containerSettings.setChangeFeedPolicy(ChangeFeedPolicy.createFullFidelityPolicy(Duration.ofMinutes(60)));

        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(10000);

        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerSettings,
                throughputProperties);
        this.createdDatabase = client2.getDatabase(database.getId());
        this.createdAsyncDatabase = clientAsync.getDatabase(createdDatabase.getId());
        this.createdContainer = client2.getDatabase(DATABASE_NAME).getContainer(COLLECTION_NAME);
        this.createdAsyncContainer = clientAsync.getDatabase(DATABASE_NAME).getContainer(COLLECTION_NAME);
        return containerResponse.getProperties();
    }

    private void createContainer(
            Function<CosmosContainerProperties, CosmosContainerProperties> onInitialization) {

        CosmosContainerProperties containerProperties = createNewCollection(client, DATABASE_NAME, COLLECTION_NAME);

        if (onInitialization != null) {
            containerProperties = onInitialization.apply(containerProperties);
        }

        this.createdContainer = createdDatabase.getContainer(COLLECTION_NAME);
        this.createdAsyncContainer = createdAsyncDatabase.getContainer(COLLECTION_NAME);
    }

}
