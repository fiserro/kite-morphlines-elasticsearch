/*
 * Copyright 2013 Sematext Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.sematext.morphline.elasticsearch;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransportDocumentLoader implements DocumentLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportDocumentLoader.class);

  public static final int DEFAULT_PORT = 9300;
  private static final int DEFAULT_RETRY_COUNT = -1;
  private static final int DEFAULT_RETRY_SLEEP = 200;

  private Collection<InetSocketTransportAddress> serverAddresses;
//  private BulkRequestBuilder bulkRequestBuilder;

  private Client client;
  private int batchSize = 1000;
//  private int batchLoad = 0;
  private List<IndexRequestBuilder> batch = new ArrayList<>(batchSize);
  private int retryCount = DEFAULT_RETRY_COUNT; // TODO by config
  private int retrySleep = DEFAULT_RETRY_SLEEP; // TODO by config
  
  public TransportDocumentLoader() {
    openLocalDiscoveryClient();
  }

  public TransportDocumentLoader(Client client) {
    this.client = client;
  }

  public TransportDocumentLoader(Collection<String> hostNames, String clusterName, int batchSize) {
    serverAddresses = new LinkedList<InetSocketTransportAddress>();
    for (String hostName : hostNames) {
      String[] hostPort = hostName.trim().split(":");
      String host = hostPort[0].trim();
      int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim()) : DEFAULT_PORT;
      serverAddresses.add(new InetSocketTransportAddress(host, port));
    }
    this.batchSize = batchSize;
    openClient(clusterName);
  }

//  @VisibleForTesting
//  public void setBulkRequestBuilder(BulkRequestBuilder bulkRequestBuilder) {
//    this.bulkRequestBuilder = bulkRequestBuilder;
//  }

  @VisibleForTesting
  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  @VisibleForTesting
  public void setRetrySleep(int retrySleep) {
    this.retrySleep = retrySleep;
  }

  @VisibleForTesting
  public BulkRequestBuilder createBulkBuilder() {
    return client.prepareBulk();
  }

  @Override
  public void beginTransaction() {
//    bulkRequestBuilder = client.prepareBulk();
//    batchLoad = 0;
    batch = new ArrayList<>(batchSize);
  }

  @Override
  public void commitTransaction() throws Exception {
    sendBatch();
  }

  private void sendBatch() {
    int tryNum = -1;
    String failureMessage = null;
    
    LOGGER.debug("Sending bulk to elasticsearch cluster");

    BulkRequestBuilder bulkRequestBuilder = createBulkBuilder();
    for (IndexRequestBuilder indexRequestBuilder : batch) {
      bulkRequestBuilder.add(indexRequestBuilder);
    }

    int numOfFaileDocuments = 0;

    while (tryNum++ < retryCount || retryCount < 0) {

      numOfFaileDocuments = 0;
      BulkResponse bulkResponse;
      try {
        bulkResponse = bulkRequestBuilder.execute().actionGet();
        if (!bulkResponse.hasFailures()) {
          failureMessage = null;
          break; // sent all
        }
      } catch (Exception e) {
        failureMessage = e.getMessage();
        numOfFaileDocuments = batch.size();
        continue; // failed all
      }
//      if (failureMessage != null && failureMessage.indexOf("\n") > -1) {
//        failureMessage = failureMessage.substring(0, failureMessage.indexOf("\n"));
//      }

      // some sent, some failed - retry only the failed
      bulkRequestBuilder = createBulkBuilder();
      failureMessage = bulkResponse.buildFailureMessage();
      for (BulkItemResponse item : bulkResponse.getItems()) {
        if (item.isFailed()) {
          numOfFaileDocuments++;
          int itemId = item.getItemId();
          bulkRequestBuilder.add(batch.get(itemId));
        }
      }
      
      String retriesLeft = retryCount < 0 ? "." : (" (" + (retryCount - tryNum) + " retries lefts).");
      LOGGER.warn("Sending " + numOfFaileDocuments + "/" + batchSize 
          + " documents to elasticserach failed because of: " + failureMessage
          + "\nThe " + (tryNum + 1) + ". retry will be send after " + retrySleep + "ms" + retriesLeft);
      try {
        Thread.sleep(retrySleep);
      } catch (InterruptedException e) {
        LOGGER.error("Waiting to next retry interupted.", e);
      }
    }

//    bulkRequestBuilder = client.prepareBulk();
    beginTransaction();
    if (numOfFaileDocuments > 0) {
      if (failureMessage != null) {
        throw new MorphlineRuntimeException(failureMessage);
      } else {
        throw new MorphlineRuntimeException("No failure message was thrown.");
      }
    }
  }

  @Override
  public void rollbackTransaction() throws IOException {
    batch = null;
  }

  @Override
  public void addDocument(BytesReference document, String index, String indexType, int ttlMs) throws Exception {
    if (batch == null) {
      beginTransaction();
    }

    IndexRequestBuilder indexRequestBuilder = null;
    indexRequestBuilder = client.prepareIndex(index, indexType).setSource(document);
    if (ttlMs > 0) {
      indexRequestBuilder.setTTL(ttlMs);
    }
    batch.add(indexRequestBuilder);

    if (batch.size() >= batchSize) {
      sendBatch();
    }
  }

  @Override
  public void shutdown() throws Exception {
    if (client != null) {
      client.close();
    }
    client = null;
  }

  /**
   * Open client to elaticsearch cluster
   *
   * @param clusterName
   */
  private void openClient(String clusterName) {
    Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();

    TransportClient transportClient = new TransportClient(settings);
    for (InetSocketTransportAddress host : serverAddresses) {
      transportClient.addTransportAddress(host);
    }
    if (client != null) {
      client.close();
    }
    client = transportClient;
  }

  /*
   * FOR TESTING ONLY. Open local connection.
   */
  private void openLocalDiscoveryClient() {
    LOGGER.info("Using ElasticSearch AutoDiscovery mode");
    Node node = NodeBuilder.nodeBuilder().client(true).local(true).node();
    if (client != null) {
      client.close();
    }
    client = node.client();
  }
}
