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

package org.grails.plugins.elasticsearch.conversion

import org.codehaus.groovy.grails.commons.GrailsDomainClassProperty
import org.elasticsearch.common.xcontent.XContentBuilder
import static org.elasticsearch.common.xcontent.XContentFactory.*
import org.codehaus.groovy.grails.orm.hibernate.cfg.GrailsHibernateUtil
import org.codehaus.groovy.grails.commons.GrailsDomainClass
import org.grails.plugins.elasticsearch.conversion.marshall.DeepDomainClassMarshaller
import org.grails.plugins.elasticsearch.conversion.marshall.DefaultMarshallingContext
import org.grails.plugins.elasticsearch.conversion.marshall.DefaultMarshaller
import org.grails.plugins.elasticsearch.conversion.marshall.MapMarshaller
import org.grails.plugins.elasticsearch.conversion.marshall.CollectionMarshaller
import org.grails.plugins.elasticsearch.conversion.marshall.GeoPointMarshaller
import org.codehaus.groovy.grails.commons.DomainClassArtefactHandler
import java.beans.PropertyEditor
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.grails.plugins.elasticsearch.conversion.marshall.PropertyEditorMarshaller
import org.grails.plugins.elasticsearch.conversion.marshall.Marshaller
import org.grails.plugins.elasticsearch.conversion.marshall.SearchableReferenceMarshaller
import org.apache.commons.lang.ClassUtils
import org.apache.commons.logging.LogFactory


/**
 * Marshall objects as JSON.
 */
public class JSONDomainFactory {
	
	private static final Set<String> SUPPORTED_FORMAT = new HashSet<String>(Arrays.asList(
		"string", "integer", "long", "float", "double", "boolean", "null", "date"));

    def elasticSearchContextHolder
    def grailsApplication
	private log = LogFactory.getLog(JSONDomainFactory)

    /**
     * The default marshallers, not defined by user
     */
    def static DEFAULT_MARSHALLERS = [
            (Map): MapMarshaller,
            (Collection): CollectionMarshaller
    ]
    
    def public static SPECIAL_MARSHALLERS = [:]
    
    /**
     * Create and use the correct marshaller for a peculiar class
     * @param object The instance to marshall
     * @param marshallingContext The marshalling context associate with the current marshalling process
     * @return Object The result of the marshall operation.
     */
    public delegateMarshalling(object, marshallingContext, nullValue, maxDepth = 0) {
        if (object == null) {
            log.debug("returning null")
            return nullValue
        }
        def marshaller = null
        def objectClass = object.getClass()

        // Resolve collections.
        // Check for direct marshaller matching
        if (object instanceof Collection) {
            marshaller = new CollectionMarshaller()
        }


        if (!marshaller && DEFAULT_MARSHALLERS[objectClass]) {
            marshaller = DEFAULT_MARSHALLERS[objectClass].newInstance()
        }

        if (!marshaller) {

            // Check if we arrived from searchable domain class.
            def parentObject = marshallingContext.peekDomainObject()
            if (parentObject && marshallingContext.lastParentPropertyName && DomainClassArtefactHandler.isDomainClass(parentObject.getClass())) {
                GrailsDomainClass domainClass = getDomainClass(parentObject)
                def propertyMapping = elasticSearchContextHolder.getMappingContext(domainClass)?.getPropertyMapping(marshallingContext.lastParentPropertyName)
                def converter = propertyMapping?.converter
                // Property has converter information. Lets see how we can apply it.
                if (converter) {
                    // Property editor?
                    if (converter instanceof Class) {
                        if (PropertyEditor.isAssignableFrom(converter)) {
                            marshaller = new PropertyEditorMarshaller(propertyEditorClass:converter)
                        }
                    }
                } else if (propertyMapping?.reference) {
                    def refClass = propertyMapping.getBestGuessReferenceType()
                    marshaller = new SearchableReferenceMarshaller(refClass:refClass)
                } else if (propertyMapping?.component) {
                    marshaller = new DeepDomainClassMarshaller()
                }
            }
        }

        if (!marshaller) {
            // TODO : support user custom marshaller/converter (& marshaller registration)
            // Check for domain classes

            if (DomainClassArtefactHandler.isDomainClass(objectClass)) {
                def propertyMapping = elasticSearchContextHolder.getMappingContext(getDomainClass(marshallingContext.peekDomainObject()))?.getPropertyMapping(marshallingContext.lastParentPropertyName)
                if (propertyMapping?.isGeoPoint()) {
                    marshaller = new GeoPointMarshaller()
                } else if (DomainClassArtefactHandler.isDomainClass(objectClass)) {
                    /*def domainClassName = objectClass.simpleName.substring(0,1).toLowerCase() + objectClass.simpleName.substring(1)
                    SearchableClassPropertyMapping propMap = elasticSearchContextHolder.getMappingContext(domainClassName).getPropertyMapping(marshallingContext.lastParentPropertyName)*/
                    marshaller = new DeepDomainClassMarshaller()
                }
            } else {
                // Check for inherited marshaller matching
                def inheritedMarshaller = DEFAULT_MARSHALLERS.find { key, value -> key.isAssignableFrom(objectClass)}
                if (inheritedMarshaller) {
                    marshaller = DEFAULT_MARSHALLERS[inheritedMarshaller.key].newInstance()
                    // If no marshaller was found, use the default one
                } else {
                    marshaller = new DefaultMarshaller()
                }
            }
        }

        marshaller.marshallingContext = marshallingContext
        marshaller.elasticSearchContextHolder = elasticSearchContextHolder
        marshaller.maxDepth = maxDepth
        log.debug("mashalling ${object}")
        marshaller.marshall(object)
    }
	

    private GrailsDomainClass getDomainClass(instance) {
        grailsApplication.domainClasses.find {it.clazz == instance.class}
    }

    /**
     * Build an XContentBuilder representing a domain instance in JSON.
     * Use as a source to an index request to ElasticSearch.
     * @param instance A domain class instance.
     * @return
     */
    public XContentBuilder buildJSON(instance) {
        def objectClass = instance.class
        
        if (SPECIAL_MARSHALLERS[objectClass]) {
            return SPECIAL_MARSHALLERS[objectClass](jsonBuilder(), instance)
        }
        
        instance = GrailsHibernateUtil.unwrapIfProxy(instance)
        def domainClass = getDomainClass(instance)
        def json = jsonBuilder().startObject()
        // TODO : add maxDepth in custom mapping (only for "seachable components")
        def scm = elasticSearchContextHolder.getMappingContext(domainClass)
        def marshallingContext = new DefaultMarshallingContext(maxDepth: 5, parentFactory: this)
        marshallingContext.push(instance)
        // Build the json-formated map that will contain the data to index
        scm.propertiesMapping.each { scpm ->
            marshallingContext.lastParentPropertyName = scpm.propertyName
            // todo figure out null value
            
            def nullObject = null
			def classType = scpm.getPropertyType()
			String shortTypeName = ClassUtils.getShortClassName(classType);
			def type = shortTypeName.substring(0,1).toLowerCase(Locale.ENGLISH) + shortTypeName.substring(1);
            log.debug("type: ${type}")
            
			if(type.equals("string")) nullObject = ""
			else if( type.equals("integer") || type.equals("long") || type.equals("float") || type.equals("double") ) 
				nullObject = 0
			else if(type.equals("set")|| type.equals("list") || type.equals("collection")) 
                nullObject = []
			else if(type.equals("map")) 
                nullObject = [:]
            
            def res = delegateMarshalling(instance."${scpm.propertyName}", marshallingContext,nullObject)
            json.field(scpm.propertyName, res)
        }
        marshallingContext.pop()
        json.endObject()
        json.close()
        json
    }
	
    public Object getInstanceProperty(instance, scpm) {
      instance."${scpm.propertyName}"
    }
}
