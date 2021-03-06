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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.bytes.BytesReference;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestDocumentLoader implements DocumentLoader {

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final String INDEX_OPERATION_NAME = "index";
  private static final String INDEX_PARAM = "_index";
  private static final String TYPE_PARAM = "_type";
  private static final String TTL_PARAM = "_ttl";
  private static final String BULK_ENDPOINT = "_bulk";

  private static final Logger LOGGER = LoggerFactory.getLogger(RestDocumentLoader.class);

  private final RoundRobinList<String> serversList;

  private StringBuilder bulkBuilder;
  private HttpClient httpClient;
  private int batchSize = DEFAULT_BATCH_SIZE;
  private int batchLoad = 0;
  
  public RestDocumentLoader() {
    serversList = new RoundRobinList<String>(Arrays.asList("http://localhost:9200"));
  }

  public RestDocumentLoader(Collection<String> hostNames, int batchSize) {
    this(hostNames, batchSize, new DefaultHttpClient());
  }

  public RestDocumentLoader(Collection<String> hostNames, HttpClient httpClient) {
    this(hostNames, DEFAULT_BATCH_SIZE, httpClient);
  }

  public RestDocumentLoader(Collection<String> hostNames, int batchSize, HttpClient httpClient) {
    List<String> hosts = new LinkedList<String>();
    for (String hostName : hostNames) {
      if (!hostName.contains("http://") && !hostName.contains("https://")) {
        hosts.add("http://" + hostName);
      }
    }

    serversList = new RoundRobinList<String>(hosts);
    bulkBuilder = new StringBuilder();
    this.httpClient = httpClient;
    this.batchSize = batchSize;
  }

  @Override
  public void beginTransaction() throws IOException {
    bulkBuilder = new StringBuilder();
    batchLoad = 0;
  }

  @Override
  public void commitTransaction() throws Exception {
    int statusCode = 0, triesCount = 0;
    HttpResponse response = null;
    LOGGER.debug("Sending bulk request to elasticsearch cluster");

    String entity;
    entity = bulkBuilder.toString();
    System.out.println(entity);
    bulkBuilder = new StringBuilder();
    batchLoad = 0;

    while (statusCode != HttpStatus.SC_OK && triesCount < serversList.size()) {
      triesCount++;
      String host = serversList.get();
      String url = host + "/" + BULK_ENDPOINT;
      HttpPost httpRequest = new HttpPost(url);
      httpRequest.setEntity(new StringEntity(entity));
      response = httpClient.execute(httpRequest);
      statusCode = response.getStatusLine().getStatusCode();
      LOGGER.debug("Status code from elasticsearch: " + statusCode);
      if (response.getEntity() != null) {
        LOGGER.debug("Status message from elasticsearch: " + EntityUtils.toString(response.getEntity(), "UTF-8"));
      }
    }

    if (statusCode != HttpStatus.SC_OK) {
      if (response.getEntity() != null) {
        throw new MorphlineRuntimeException(EntityUtils.toString(response.getEntity(), "UTF-8"));
      } else {
        throw new MorphlineRuntimeException("Elasticsearch status code was: " + statusCode);
      }
    }
  }

  @Override
  public void rollbackTransaction() throws IOException {
    bulkBuilder = new StringBuilder();
    batchLoad = 0;
  }

  @Override
  public void addDocument(BytesReference document, String index, String indexType, String id, long ttlMs) throws Exception {
    Map<String, Map<String, String>> parameters = new HashMap<String, Map<String, String>>();
    Map<String, String> indexParameters = new HashMap<String, String>();
    indexParameters.put(INDEX_PARAM, index);
    indexParameters.put(TYPE_PARAM, indexType);
    indexParameters.put("_id", id);
    if (ttlMs > 0) {
      indexParameters.put(TTL_PARAM, Long.toString(ttlMs));
    }
    parameters.put("update", indexParameters);

    Map<String, Object> doc = new HashMap<>();
    doc.put("doc_as_upsert", true);
    bulkBuilder.append(jsonBuilder().value(parameters).bytes().toUtf8());
    bulkBuilder.append("\n");
    bulkBuilder.append("{\"doc\":");
    bulkBuilder.append(document.toBytesArray().toUtf8());
    bulkBuilder.append(", \"doc_as_upsert\":true}");
    bulkBuilder.append("\n");

    if (++batchLoad >= batchSize) {
      commitTransaction();
    }
  }

  @Override
  public void shutdown() throws Exception {
    bulkBuilder = null;
  }
}
