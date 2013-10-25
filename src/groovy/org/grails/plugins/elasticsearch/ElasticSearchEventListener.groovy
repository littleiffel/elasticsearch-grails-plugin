/*
 * Copyright 2002-2010 the original author or authors.
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
package org.grails.plugins.elasticsearch

import org.springframework.context.ApplicationContextAware
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationEvent;
import org.grails.plugins.elasticsearch.index.IndexRequestQueue
import org.apache.log4j.Logger
import org.grails.datastore.mapping.core.Datastore
import org.grails.datastore.mapping.engine.EntityAccess
import org.grails.datastore.mapping.engine.event.AbstractPersistenceEvent
import org.grails.datastore.mapping.engine.event.AbstractPersistenceEventListener
import org.grails.datastore.mapping.engine.event.PostDeleteEvent
import org.grails.datastore.mapping.engine.event.PostInsertEvent
import org.grails.datastore.mapping.engine.event.PostUpdateEvent
import org.grails.datastore.mapping.engine.event.PreInsertEvent
import org.grails.datastore.mapping.engine.event.PreUpdateEvent
import org.grails.datastore.mapping.model.PersistentEntity

/**
 * Listen to Hibernate events.
 */
class ElasticSearchEventListener extends AbstractPersistenceEventListener {
    /** Logger */
    private static final Logger LOG = Logger.getLogger(ElasticSearchEventListener.class)

    /** ES context */
    def elasticSearchContextHolder

    /** Spring application context */
    def applicationContext
    
    public ElasticSearchEventListener(final Datastore datastore, ApplicationContext appContext, ElasticSearchContextHolder contextHolder) {
      super(datastore)
      
      applicationContext = appContext
      elasticSearchContextHolder = contextHolder
    }

    IndexRequestQueue getIndexRequestQueue() {
        applicationContext.getBean("indexRequestQueue", IndexRequestQueue)
    }

    /**
     * Push object to index.
     * @param obj object to index
     */
    def pushToIndex(obj) {
      indexRequestQueue.addIndexRequest(obj)
    }

    /**
     * Push object to delete.
     * @param obj object to delete
     */
    def pushToDelete(obj) {
      indexRequestQueue.addDeleteRequest(obj)
    }

    
    @Override
    protected void onPersistenceEvent(final AbstractPersistenceEvent event) {
      if (event instanceof PostInsertEvent) {
        postInsert(event)
      }
      else if (event instanceof PostUpdateEvent) {
        postUpdate(event)
      }
      else if (event instanceof PostDeleteEvent) {
        postDelete(event)
      }
    }
  
    public boolean supportsEventType(Class<? extends ApplicationEvent> eventType) {
      return true
    }


    void postInsert(AbstractPersistenceEvent event) {
        def clazz = event.entityObject.class
        if (elasticSearchContextHolder.isRootClass(clazz)) {
            pushToIndex(event.entityObject)
        }
        
        indexRequestQueue.executeRequests()
    }

    void postUpdate(AbstractPersistenceEvent event) {
        def clazz = event.entityObject.class
        if (elasticSearchContextHolder.isRootClass(clazz)) {
            pushToIndex(event.entityObject)
        }
        
        indexRequestQueue.executeRequests()
    }

    void postDelete(AbstractPersistenceEvent event) {
        def clazz = event.entityObject.class
        if (elasticSearchContextHolder.isRootClass(clazz)) {
            pushToDelete(event.entityObject)
        }
        
        indexRequestQueue.executeRequests()
    }

    void setApplicationContext(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext
    }
}
