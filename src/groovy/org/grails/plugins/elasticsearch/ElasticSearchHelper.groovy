package org.grails.plugins.elasticsearch

import org.elasticsearch.client.Client
import org.elasticsearch.groovy.client.GClient

/**
 *
 * @author Graeme Rocher
 */
class ElasticSearchHelper {

  Client elasticSearchClient

  def withElasticSearch(Closure callable) {
    callable.call(elasticSearchClient)
  }

}
