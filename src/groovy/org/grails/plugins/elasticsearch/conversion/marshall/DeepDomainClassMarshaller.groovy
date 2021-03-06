package org.grails.plugins.elasticsearch.conversion.marshall

import org.codehaus.groovy.grails.commons.GrailsDomainClassProperty
import org.codehaus.groovy.grails.commons.GrailsDomainClass
import org.codehaus.groovy.grails.orm.hibernate.cfg.GrailsHibernateUtil
import org.codehaus.groovy.grails.commons.DomainClassArtefactHandler
import org.springframework.beans.BeansException
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.apache.commons.lang.ClassUtils


class DeepDomainClassMarshaller extends DefaultMarshaller {
    
  private ApplicationContext ctx
  
  protected doMarshall(instance) {
    instance = GrailsHibernateUtil.unwrapIfProxy(instance)
    def domainClass = getDomainClass(instance)
    // don't use instance class directly, instead unwrap from javaassist
    println('active domain class:' + domainClass)
    def marshallResult = [id: instance.id, 'class': domainClass.clazz.name]
    def scm = elasticSearchContextHolder.getMappingContext(domainClass)
    if (!scm) {
        throw new IllegalStateException("Domain class ${domainClass} is not searchable.")
    }
    for (GrailsDomainClassProperty prop in domainClass.properties) {
      def propertyMapping = scm.getPropertyMapping(prop.name)
      if (!propertyMapping) {
        continue
      }
      def propertyClassName = instance."${prop.name}"?.class?.name
      def propertyClass = instance."${prop.name}"?.class
      def propertyValue = instance."${prop.name}"

      // get correct null value - TODO
	  def nullObject = null
      
	  def classType = propertyMapping.getPropertyType()
	  String shortTypeName = ClassUtils.getShortClassName(classType);
	  def type = shortTypeName.substring(0,1).toLowerCase(Locale.ENGLISH) + shortTypeName.substring(1);
	  if(type.equals("string")) nullObject = new String() 
	  else if( type.equals("long") || type.equals("integer") || type.equals("double") || type.equals("float") ) nullObject = 0
	  else if(type.equals("set")|| type.equals("list") || type.equals("collection")) nullObject = []
      else if(type.equals("map")) nullObject = [:]
	  log.debug("type: ${type}")
      
      // Domain marshalling
      if (DomainClassArtefactHandler.isDomainClass(propertyClass)) {
        if (propertyValue.class.searchable) {   // todo fixme - will throw exception when no searchable field.
          marshallingContext.lastParentPropertyName = prop.name
          marshallResult += [(prop.name): ([id: propertyValue.ident(), 'class': propertyClassName] + marshallingContext.delegateMarshalling(propertyValue,nullObject, propertyMapping.maxDepth))]
        } else {
          marshallResult += [(prop.name): [id: propertyValue.ident(), 'class': propertyClassName]]
        }

        // Non-domain marshalling
      } else {
        marshallingContext.lastParentPropertyName = prop.name
        def marshalledValue = marshallingContext.delegateMarshalling(propertyValue,nullObject)
        // Ugly XContentBuilder bug: it only checks for EXACT class match with java.util.Date
        // (sometimes it appears to be java.sql.Timestamp for persistent objects)
        if (marshalledValue instanceof java.util.Date) {
            marshalledValue = new java.util.Date(marshalledValue.getTime())
        }
        marshallResult += [(prop.name): marshalledValue]
      }
    }
    return marshallResult
  }
  
  protected nullValue(){
    return []
  }

  private GrailsDomainClass getDomainClass(instance) {
    println('instanceClass: ' + instance.class)
    println('available domainClasses:' + elasticSearchContextHolder.grailsApplication.domainClasses)
	  def instanceClass = instance.class
	  elasticSearchContextHolder.grailsApplication.domainClasses.find {it.clazz == instanceClass}
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
  	this.ctx = applicationContext
  }
}
