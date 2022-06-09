// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.azure.cosmos.examples.changefeed;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.examples.common.CustomPOJO2;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.implementation.apachecommons.lang.RandomStringUtils;
import com.azure.cosmos.models.ChangeFeedPolicy;
import com.azure.cosmos.models.CosmosChangeFeedRequestOptions;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.FeedRange;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Sample for Change Feed Processor.
 * This sample models an application where documents are being inserted into one
 * container (the "feed container"),
 * and meanwhile another worker thread or worker application is pulling inserted
 * documents from the feed container's Change Feed
 * and operating on them in some way. For one or more workers to process the
 * Change Feed of a container, the workers must first contact the server
 * and "lease" access to monitor one or more partitions of the feed container.
 * The Change Feed Processor Library
 * handles leasing automatically for you, however you must create a separate
 * "lease container" where the Change Feed
 * Processor Library can store and track leases container partitions.
 */
public class SimpleFullFidelityChangeFeed {

    public static int WAIT_FOR_WORK = 60000;
    // public static final String DATABASE_NAME = "db_" +
    // RandomStringUtils.randomAlphabetic(7);
    // public static final String COLLECTION_NAME = "coll_" +
    // RandomStringUtils.randomAlphabetic(7);
    public static final String DATABASE_NAME = "db5";
    public static final String COLLECTION_NAME = "ffcf";
    private static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();
    protected static Logger logger = LoggerFactory.getLogger(SimpleFullFidelityChangeFeed.class);

    // private static ChangeFeedProcessor changeFeedProcessorInstance;
    // private static boolean isWorkCompleted = false;

    public static void main(String[] args) {
        logger.info("BEGIN Sample");

        try {

            logger.info("-->CREATE DocumentClient");
            CosmosAsyncClient client = getCosmosClient();

            logger.info("-->CREATE sample's database: " + DATABASE_NAME);
            CosmosAsyncDatabase cosmosDatabase = createNewDatabase(client, DATABASE_NAME);

            logger.info("-->CREATE container for documents: " + COLLECTION_NAME);
            CosmosAsyncContainer feedContainer = createNewCollection(client, DATABASE_NAME, COLLECTION_NAME);

            logger.info("-->CREATE container for lease: " + COLLECTION_NAME + "-leases");
            CosmosAsyncContainer leaseContainer = createNewLeaseCollection(client, DATABASE_NAME,
                    COLLECTION_NAME + "-leases");

            logger.info("-->START Change Feed Processor on worker (handles changes asynchronously)");

            AtomicReference<String> continuation = new AtomicReference<>();
            CosmosChangeFeedRequestOptions options = CosmosChangeFeedRequestOptions
                    .createForProcessingFromNow(FeedRange.forFullRange());
            logger.info("-->START application that inserts documents into feed container");
            createNewDocumentsCustomPOJO(feedContainer, 2, Duration.ofSeconds(1));

            List<CustomPOJO2> results = feedContainer
                    .queryChangeFeed(options, CustomPOJO2.class)
                    .handle((r) -> continuation.set(r.getContinuationToken()))
                    .collectList()
                    .block();

            logger.info("results: " + results);

            for (CustomPOJO2 objectNode : results) {
                logger.info("doc: " + objectNode);
                // System.out.print("doc: "+objectNode);
            }

            logger.info("-->DELETE sample's database: " + DATABASE_NAME);
            deleteDatabase(cosmosDatabase);

            Thread.sleep(500);

        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.info("END Sample");
    }

    public static CosmosAsyncClient getCosmosClient() {

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

    public static void deleteDatabase(CosmosAsyncDatabase cosmosDatabase) {
        cosmosDatabase.delete().block();
    }

    public static CosmosAsyncContainer createNewCollection(CosmosAsyncClient client, String databaseName,
            String collectionName) {
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer collectionLink = databaseLink.getContainer(collectionName);
        CosmosContainerResponse containerResponse = null;

        try {
            containerResponse = collectionLink.read().block();

        } catch (RuntimeException ex) {
            if (ex instanceof CosmosException) {
                CosmosException CosmosException = (CosmosException) ex;

                if (CosmosException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerProperties containerSettings = new CosmosContainerProperties(collectionName, "/pk");

        containerSettings.setChangeFeedPolicy(ChangeFeedPolicy.createFullFidelityPolicy(Duration.ofMinutes(60)));
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();

        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(10000);

        containerResponse = databaseLink.createContainer(containerSettings, throughputProperties, requestOptions)
                .block();

        return databaseLink.getContainer(containerResponse.getProperties().getId());
    }

    public static CosmosAsyncContainer createNewLeaseCollection(CosmosAsyncClient client, String databaseName,
            String leaseCollectionName) {
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer leaseCollectionLink = databaseLink.getContainer(leaseCollectionName);
        CosmosContainerResponse leaseContainerResponse = null;

        try {
            leaseContainerResponse = leaseCollectionLink.read().block();

            if (leaseContainerResponse != null) {
                leaseCollectionLink.delete().block();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        } catch (RuntimeException ex) {
            if (ex instanceof CosmosException) {
                CosmosException CosmosException = (CosmosException) ex;

                if (CosmosException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerProperties containerSettings = new CosmosContainerProperties(leaseCollectionName, "/id");
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();

        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        leaseContainerResponse = databaseLink.createContainer(containerSettings, throughputProperties, requestOptions)
                .block();

        if (leaseContainerResponse == null) {
            throw new RuntimeException(
                    String.format("Failed to create collection %s in database %s.", leaseCollectionName, databaseName));
        }

        return databaseLink.getContainer(leaseContainerResponse.getProperties().getId());
    }

    public static void createNewDocumentsCustomPOJO(CosmosAsyncContainer containerClient, int count, Duration delay) {
        String suffix = RandomStringUtils.randomAlphabetic(3);
        for (int i = 0; i <= count; i++) {
            CustomPOJO2 document = new CustomPOJO2();
            document.setId(String.format("0%d-%s", i, suffix));
            document.setPk(document.getId()); // This is a very simple example, so we'll just have a partition key (/pk)
                                              // field that we set equal to id

            containerClient.createItem(document).subscribe(doc -> {
                logger.info("---->DOCUMENT WRITE: " + doc);
            });

            long remainingWork = delay.toMillis();
            try {
                while (remainingWork > 0) {
                    Thread.sleep(100);
                    remainingWork -= 100;
                }
            } catch (InterruptedException iex) {
                // exception caught
                break;
            }
        }
    }
}