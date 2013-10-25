/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.grails.plugins.elasticsearch.index;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.codehaus.groovy.runtime.InvokerHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.grails.plugins.elasticsearch.ElasticSearchContextHolder;
import org.grails.plugins.elasticsearch.conversion.JSONDomainFactory;
import org.grails.plugins.elasticsearch.exception.IndexException;
import org.grails.plugins.elasticsearch.mapping.SearchableClassMapping;
import org.grails.plugins.elasticsearch.mapping.SearchableClassPropertyMapping;
import org.springframework.beans.factory.InitializingBean;

/**
 * Holds objects to be indexed.
 * <br/>
 * It looks like we need to keep object references in memory until they indexed properly.
 * If indexing fails, all failed objects are retried. Still no support for max number of retries (todo)
 * NOTE: if cluster state is RED, everything will probably fail and keep retrying forever.
 * NOTE: This is shared class, so need to be thread-safe.
 */
public class IndexRequestQueue implements InitializingBean {

    private static final Logger LOG = Logger.getLogger(IndexRequestQueue.class);

    private JSONDomainFactory jsonDomainFactory;
    private ElasticSearchContextHolder elasticSearchContextHolder;
    private Client elasticSearchClient;
    
    /**
     * A map containing the pending index requests.
     */
    ConcurrentHashMap<IndexEntityKey, XContentBuilder> indexRequests = new ConcurrentHashMap<IndexEntityKey,XContentBuilder>();
    /**
     * A set containing the pending delete requests.
     */
    ConcurrentHashMap<IndexEntityKey, Boolean> deleteRequests = new ConcurrentHashMap<IndexEntityKey, Boolean>();

    ConcurrentLinkedQueue<OperationBatch> operationBatchQueue = new ConcurrentLinkedQueue<OperationBatch>();

    /**
     * No-args constructor.
     */
    public IndexRequestQueue() {
    }

    public void setJsonDomainFactory(JSONDomainFactory jsonDomainFactory) {
        this.jsonDomainFactory = jsonDomainFactory;
    }

    public void setElasticSearchContextHolder(ElasticSearchContextHolder elasticSearchContextHolder) {
        this.elasticSearchContextHolder = elasticSearchContextHolder;
    }

    public void setElasticSearchClient(Client elasticSearchClient) {
        this.elasticSearchClient = elasticSearchClient;
    }
    
    public void afterPropertiesSet() throws Exception {
      
    }
    
    public void addIndexRequest(Object instance) {
        addIndexRequest(instance, null);
    }

    public void addIndexRequest(Object instance, Serializable id) {
        IndexEntityKey key = id == null ? new IndexEntityKey(instance) :
          new IndexEntityKey(id.toString(), instance.getClass());
        indexRequests.put(key, toJSON(instance));
    }

    public void addDeleteRequest(Object instance) {
        deleteRequests.put(new IndexEntityKey(instance), true);
    }

    public XContentBuilder toJSON(Object instance) {
        try {
            XContentBuilder  b = jsonDomainFactory.buildJSON(instance);
            return b;
        } catch (Exception e) {
            throw new IndexException("Failed to marshall domain instance [" + instance + "]", e);
        }
    }

    public String getInstanceParentId(Object instance, SearchableClassPropertyMapping scpm) {
      Object parent = jsonDomainFactory.getInstanceProperty(instance, scpm);
      Object _id = InvokerHelper.invokeMethod(parent, "ident", null);
      Class<?> clazz = parent.getClass();
      
      if(_id==null) {
          String guid = (String) InvokerHelper.invokeMethod(parent, "getGuid", null);
          throw new IllegalArgumentException("Class " + clazz + " does not have an id. guid: " + guid);
      }
      
      return _id.toString();
    }
    
    /**
     * Execute pending requests and clear both index & delete pending queues.
     *
     * @return Returns an OperationBatch instance which is a listener to the last executed bulk operation. Returns NULL
     *         if there were no operations done on the method call.
     */
    public OperationBatch executeRequests() {
        Map<IndexEntityKey, XContentBuilder> toIndex = new HashMap<IndexEntityKey, XContentBuilder>();
        Set<IndexEntityKey> toDelete = new HashSet<IndexEntityKey>();

        cleanOperationBatchList();

        synchronized(toIndex) {
          // Copy existing queue to ensure we are not interfering with incoming requests.
          toIndex.putAll(indexRequests);
          indexRequests.clear();
        }
        
        synchronized(toDelete) {
          toDelete.addAll(deleteRequests.keySet());
          deleteRequests.clear();
        }
        
        // If there are domain instances that are both in the index requests & delete requests list,
        // they are directly deleted.
        toIndex.keySet().removeAll(toDelete);

        // If there is nothing in the queues, just stop here
        if (toIndex.isEmpty() && toDelete.isEmpty()) {
          return null;
        }

        BulkRequestBuilder bulkRequestBuilder = elasticSearchClient.prepareBulk();
        //bulkRequestBuilder.setRefresh(true);

        // Execute index requests
        for (Map.Entry<IndexEntityKey, XContentBuilder> entry : toIndex.entrySet()) {
            SearchableClassMapping scm = elasticSearchContextHolder.getMappingContextByType(entry.getKey().getClazz());
            try {
                XContentBuilder json = entry.getValue();
                
                IndexRequestBuilder builder = elasticSearchClient.prepareIndex()
                    .setIndex(scm.getIndexName())
                    .setType(scm.getElasticTypeName())
                    .setId(entry.getKey().getId()) // TODO : Composite key ?
                    .setSource(json);
                
                if(entry.getKey().getParentId() != null)
                {
                  builder.setParent(entry.getKey().getParentId());
                }
                
                bulkRequestBuilder.add(builder);
                LOG.debug("bulkRequestBuilder created");
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Indexing " + entry.getKey().getClazz() + "(index:" + scm.getIndexName() + ",type:" + scm.getElasticTypeName() +
                            ") of id " + entry.getKey().getId() + " and source " + json.string());
                }
            } catch(Exception e) {
              LOG.error("Exception thrown trying to process ElasticSearch index queue: " + e.toString());
            }
        }

        // Execute delete requests
        for (IndexEntityKey key : toDelete) {
            SearchableClassMapping scm = elasticSearchContextHolder.getMappingContextByType(key.getClazz());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Deleting object from index " + scm.getIndexName() + " and type " + scm.getElasticTypeName() + " and ID " + key.getId());
            }
            bulkRequestBuilder.add(
                    elasticSearchClient.prepareDelete()
                            .setIndex(scm.getIndexName())
                            .setType(scm.getElasticTypeName())
                            .setId(key.getId())
            );
        }

        // Perform bulk request
        OperationBatch completeListener = null;
        if (bulkRequestBuilder.numberOfActions() > 0) {
            completeListener = new OperationBatch(2, toIndex, toDelete);
            operationBatchQueue.add(completeListener);
            try {
                bulkRequestBuilder.execute().addListener(completeListener);
            } catch (Exception e) {
                throw new IndexException("Failed to index/delete " + bulkRequestBuilder.numberOfActions(), e);
            }
        }
        
        LOG.debug("returning from execRequests");

        return completeListener;
    }

    public void waitComplete() {
      LOG.debug("IndexRequestQueue.waitComplete() called");

      // our iterator is guaranteed to reflect the queue at construction and not throw concurrent modification exceptions
      for (Iterator<OperationBatch> opIter = operationBatchQueue.iterator(); opIter.hasNext();) {
        OperationBatch op = opIter.next();
        
        op.waitComplete();
        opIter.remove();
      }
    }

    private void cleanOperationBatchList() {
      for (Iterator<OperationBatch> opIter = operationBatchQueue.iterator(); opIter.hasNext();) {
        OperationBatch op = opIter.next();

        if (op.isComplete()) {
          opIter.remove();
        }
      }

      LOG.debug("OperationBatchList cleaned");
    }

    class OperationBatch implements ActionListener<BulkResponse> {

		private int attempts;
        private Map<IndexEntityKey, XContentBuilder> toIndex;
        private Set<IndexEntityKey> toDelete;
        private CountDownLatch synchronizedCompletion = new CountDownLatch(1);

        OperationBatch(int attempts, Map<IndexEntityKey, XContentBuilder> toIndex, Set<IndexEntityKey> toDelete) {
            this.attempts = attempts;
            this.toIndex = toIndex;
            this.toDelete = toDelete;
        }

        public boolean isComplete() {
            return synchronizedCompletion.getCount() == 0;
        }

        public void waitComplete() {
            waitComplete(null);
        }

        /**
         * Wait for the operation to complete. Use this method to synchronize the application with the last ES operation.
         *
         * @param msTimeout A maximum timeout (in milliseconds) before the wait method returns, whether the operation has been completed or not.
         *                  Default value is 5000 milliseconds
         */
        public void waitComplete(Integer msTimeout) {
            msTimeout = msTimeout == null ? 5000 : msTimeout;
            
            try {
                if(!synchronizedCompletion.await(msTimeout, TimeUnit.MILLISECONDS)) {
                    LOG.warn("OperationBatchList.waitComplete() timed out after " + msTimeout.toString() + "ms");
                }
            } catch (InterruptedException ie) {
                LOG.warn("OperationBatchList.waitComplete() interrupted");
            }
        }

        public void fireComplete() {
            synchronizedCompletion.countDown();
        }

        public void onResponse(BulkResponse bulkResponse) {
            for (BulkItemResponse item : bulkResponse.getItems()) {
                boolean removeFromQueue = !item.isFailed()
                        || item.getFailureMessage().indexOf("UnavailableShardsException") >= 0 
                        || item.getFailureMessage().indexOf("IndexShardMissingException") >= 0;
                // On shard failure, do not re-push.
                if (removeFromQueue) {
                    if(item.getFailureMessage()!=null)
                    LOG.error("Elastic type [" + item.getType() + "] has failed: " + item.getFailureMessage());
                    // remove successful OR fatal ones.
                    Class<?> entityClass = elasticSearchContextHolder.findMappedClassByElasticType(item.getType());
                    if (entityClass == null) {
                        LOG.error("Elastic type [" + item.getType() + "] is not mapped.");
                        continue;
                    }
                    IndexEntityKey key = new IndexEntityKey(item.getId(), entityClass);
                    toIndex.remove(key);
                    toDelete.remove(key);
                }
                if (item.isFailed()) {
                    LOG.error("Failed bulk item: " + item.getFailureMessage());
                }
            }
            if (!toIndex.isEmpty() || !toDelete.isEmpty()) {
                LOG.debug("Test to push if it is empty");
                LOG.debug("bulkResponse: " + bulkResponse.toString());
                
                if(bulkResponse.hasFailures()) {
                	LOG.debug("Not pushing failure message: " + bulkResponse.buildFailureMessage());
                    LOG.error("Failed bulkResponse not pushing.");
                	fireComplete();
                } else {
                	push();
                }
                	
            } else {
                fireComplete();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Batch complete: " + bulkResponse.getItems().length + " actions.");
                }
            }
        }

        public void onFailure(Throwable e) {
            // Everything failed. Re-push all.
            LOG.error("Bulk request onFailure", e);
            
            if(attempts > 0) {
            	push();
            	attempts = attempts - 1;
            } else {
            	LOG.debug("no more trys. attempts have run out");
            }
        }


        /**
         * Push specified entities to be retried.
         */
        public void push() {
            LOG.debug("Pushing retry: " + toIndex.size() + " indexing, " + toDelete.size() + " deletes.");
            for (Map.Entry<IndexEntityKey, XContentBuilder> entry : toIndex.entrySet()) {
                indexRequests.putIfAbsent(entry.getKey(), entry.getValue());
            }
            for (IndexEntityKey key : toDelete) {
                deleteRequests.put(key, true);
            }

            executeRequests();
        }
    }

	class IndexEntityKey implements Serializable {

		private static final long serialVersionUID = -4266229881141586096L;
		/**
         * stringified id.
         */
        private final String id;
        private final Class<?> clazz;
        private final String parentId;
        
        IndexEntityKey(String id, Class<?> clazz) {
            this.id = id;
            this.clazz = clazz;
            this.parentId = null;
        }

        IndexEntityKey(Object instance) {
            this.clazz = instance.getClass();
            SearchableClassMapping scm = elasticSearchContextHolder.getMappingContextByType(this.clazz);
            if (scm == null) {
                throw new IllegalArgumentException("Class " + clazz + " is not a searchable domain class.");
            }
            Object _id = InvokerHelper.invokeMethod(instance, "ident", null);
            if(_id==null) {
            	String guid = (String) InvokerHelper.invokeMethod(instance, "getGuid", null);
                throw new IllegalArgumentException("Class " + clazz + " does not have an id. guid: " + guid);
            }
            this.id = _id.toString();
            
            SearchableClassPropertyMapping parent = scm.getParent();
            if (parent != null) {
              String parentId = getInstanceParentId(instance, parent);
              LOG.debug("Parent id for " + this.clazz + " was " + parentId);
              this.parentId = parentId;
            } else {
              this.parentId = null;
            }
        }

        public String getId() {
            return id;
        }

        public Class<?> getClazz() {
            return clazz;
        }
        
        public String getParentId() {
            return parentId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IndexEntityKey that = (IndexEntityKey) o;

            if (!clazz.equals(that.clazz)) return false;
            if (!id.equals(that.id)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + clazz.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "IndexEntityKey{" +
                    "id=" + id +
                    ", clazz=" + clazz +
                    '}';
        }
    }
}
