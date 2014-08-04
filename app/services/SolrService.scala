package services

import models.SolrDocument
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.response.QueryResponse
import org.apache.solr.client.solrj.impl.HttpSolrServer
import org.apache.solr.common.SolrDocumentList

// from solr_helper.rb
class SolrService {
    val solrSearchParamsLogic = List(  "defaultSolrParameters" ,
                                    "addQueryToSolr" ,
                                    "addFacetFqToSolr" /*,
                                    "add_facetting_to_solr",
                                    "add_solr_fields_to_query",
                                    "add_paging_to_solr",
                                    "add_sorting_to_solr" */)

    /**
     * a solr query method
     * given a user query, return a solr response containing both result docs and facets
     *   - mixes in the spelling suggestions
     *   - the response will have a spellingSuggestions method
     * Returns a two-element array (aka duple) with first the solrResponse object,
     * and second an array of SolrDocuments representing the response.docs
     *
     * @param  userParams  user query parameters
     */
    def getSearchResults(userParams){
        def solrResponse = querySolr(userParams)
        def documentList = null//solrResponse.docs.collect{new SolrDocument(it, solrResponse)}
        [solrResponse, documentList]
    }

    /*
     * a solr query method
     * given a user query,
     * Returns a solr response object
     */
    def querySolr(userParams){
        long benchStart = System.currentTimeMillis()
        def extraControllerParams = grailsApplication.config.gaseline.extraControllerParams
        def parameters = solrSearchParams(userParams)
        parameters.putAll(extraControllerParams)

        parameters["qt"] = parameters["qt"] ?: grailsApplication.config.gaseline.qt

        def path = grailsApplication.config.gaseline.solrPath
        if(parameters["start"]) {
            throw new Exception ("don't set start, use page and rows instead")
        }
        def rows = parameters["rows"]
        if (rows){
            parameters.remove("rows") // only transmit rows once
            if(rows instanceof String){
                rows = Integer.parseInt(rows)
            }
        }

        def page = parameters["page"]
        if(page){
            parameters.remove("page") // only transmit page once
            if(page instanceof String){
                page = Integer.parseInt(page)
            }

        } else {
            page = 1
        }
        //(page, per_page, path, opts=null){
        def solrResponse = Solr.paginate(page, rows, path, parameters)
        if(log.debugEnabled){
            log.debug("Solr query: ${parameters}")
            long benchEnd = System.currentTimeMillis()
            long duration = benchEnd - benchStart
            log.debug("Solr fetch: ${this.class}#querySolr (${sprintf('%.1f',((float) duration))} ms)" )
        }
        solrResponse
    }

    /*
     * returns a params hash for searching solr.
     * The CatalogController #index action uses this.
     * Solr parameters can come from a number of places. From lowest
     * precedence to highest:
     * 1. General defaults in blacklight config (are trumped by)
     * 2. defaults for the particular search field identified by  params["search_field"] (are trumped by)
     * 3. certain parameters directly on input HTTP query params
     *     * not just any parameter is grabbed willy nilly, only certain ones are allowed by HTTP input)
     *     * for legacy reasons, qt in http query does not over-ride qt in search field definition default.
     * 4.  extra parameters passed in as argument.
     * spellcheck.q will be supplied with the [:q] value unless specifically
     * specified otherwise.
     *
     * Incoming parameter :f is mapped to :fq solr parameter.
     *
     * @param  userParams  user query parameters
     */
    def solrSearchParams(userParams){
        def solrParameters = [ : ]
        solrSearchParamsLogic.each { methodName ->
            this."$methodName"(solrParameters, userParams)
        }
        solrParameters
    }

    /*
     * Start with general defaults from gaseline config. Need to use custom
     * merge to dup values, to avoid later mutating the original by mistake.
     */
    def defaultSolrParameters(solrParameters, userParameters){
        def defaultSolrParameters = grailsApplication.config.gaseline.defaultSolrParams
        solrParameters.putAll(defaultSolrParameters)
    }

    /*
     * Take the user-entered query, and put it in the solr params,
     * including config's "search field" params for current search field.
     * also include setting spellcheck.q.
     */
    def addQueryToSolr(solrParameters, userParameters){
        /*
         ***
         * Merge in search field configured values, if present, over-writing general
         * defaults
         ***
         * legacy behavior of user param :qt is passed through, but over-ridden
         * by actual search field config if present. We might want to remove
         * this legacy behavior at some point. It does not seem to be currently
         * rspec'd.
         */
         if(userParameters["qt"]){
            solrParameters["qt"] = userParameters["qt"]
         }
         def searchFieldDef = searchFieldDefForKey(userParameters["search_field"])
         if (searchFieldDef){
            solrParameters["qt"] = searchFieldDef.qt
            if(searchFieldDef.solr_parameters)
                solrParameters.putAll(searchFieldDef.solr_parameters)
         }

        /*
         **
         * Create Solr 'q' including the user-entered q, prefixed by any
         * solr LocalParams in config, using solr LocalParams syntax.
         * http://wiki.apache.org/solr/LocalParams
         * *
         */
        def hash
        if (searchFieldDef && (hash = searchFieldDef.solr_local_parameters)){
            def localParams = hash.collect { key, value -> "$key='$value'"}.join(" ")
            solrParameters["q"] = "{!${localParams}}${userParameters['q']}"
        } else {
            if(userParameters["q"]){
                solrParameters["q"] = userParameters["q"]
            }
        }

        /*
         * Set Solr spellcheck.q to be original user-entered query, without
         * our local params, otherwise it'll try and spellcheck the local
         * params! Unless spellcheck.q has already been set by someone,
         * respect that.
         *
         * TODO: Change calling code to expect this as a symbol instead of
         * a string, for consistency? :'spellcheck.q' is a symbol. Right now
         * rspec tests for a string, and can't tell if other code may
         * insist on a string.
         */
         if(!solrParameters["spellcheck.q"]){
             solrParameters["spellcheck.q"] = userParameters["q"]
         }
         solrParameters
    }

    /*
        ##
    # Add any existing facet limits, stored in app-level HTTP query
    # as :f, to solr as appropriate :fq query.
    def add_facet_fq_to_solr(solr_parameters, user_params)

      # convert a String value into an Array
      if solr_parameters[:fq].is_a? String
        solr_parameters[:fq] = [solr_parameters[:fq]]
      end

      # :fq, map from :f.
      if ( user_params[:f])
        f_request_params = user_params[:f]

        solr_parameters[:fq] ||= []

        f_request_params.each_pair do |facet_field, value_list|
          Array(value_list).each do |value|
            solr_parameters[:fq] << facet_value_to_fq_string(facet_field, value)
          end
        end
      end
    end
     */

   /*
    *
    * Add any existing facet limits, stored in app-level HTTP query
    * as :f, to solr as appropriate :fq query.
    */
    def addFacetFqToSolr(solrParameters, userParameters)  {
        // convert a String value into an Array
        if(solrParameters["fq"] instanceof String){
            solrParameters["fq"] = [solrParameters["fq"]]
        }

        //fq, map from f.
        if(userParameters["f"]){
            solrParameters["fq"] = [:]
            def fRequestParams = userParameters["f"]

            fRequestParams.each { facetField, valueList ->
                valueList.toArray().each { value ->
                    solrParameters["fq"] << facetValueToFqString(facetField, value)
                }
            }
        }

    }

   /*
    * Convert a facet/value pair into a solr fq parameter
    */
    def facetValueToFqString(facetField, value){
        def facetConfig = grailsApplication.config.gaseline.facet_fields[facetField]

        def localParams = []
        if(facetConfig && facetConfig.tag){
            localParams << "tag=${facetConfig.tag}"
        }

        def prefix = ""
        if(localParams){
            prefix = "{!${localParams.join(" ")}}"
        }

        def fq = null

        if (facetConfig && facetConfig.query){
            fq = facetConfig.query[value].fq
        }else if (facetConfig && facetConfig.date){
            if ((value instanceof Boolean || value == 'true' || value == 'false') &&
                (value instanceof Integer ||Integer.parseInt (value).toString() == value ) &&
                (value instanceof Float || Float.parseFloat(value).toString() == value ) &&
                (value instanceof Date   ))
            fq = "${prefix}${facetField}:${value}"
        } else if ( value instanceof Range) {
            fq = "${prefix}${facetField}:[${value.first} TO ${value.last}]"
        } else {
            if(localParams){
                fq = "{!raw f=${facetField}${(" " + localParams.join(" "))}}${value}"
            }
        }

        fq
    }

   /*
    * Looks up a search field gaseline_config hash from searchFields having
    * a certain supplied key.
    */
    private def searchFieldDefForKey(key){
//        for(sf in grailsApplication.config.gaseline.searchFields){
//            if(sf.key == "key" && sf.value == key){
//                return sf
//            }
//        }
        grailsApplication.config.gaseline.searchFields[key]
    }

}

class Solr {
    private static HttpSolrServer solr

    static def getSolr(){
        if(!solr){
            def grailsApplication = new Bookmark().domainClass.grailsApplication
            solr = new HttpSolrServer(grailsApplication.config.gaseline.solrUrl)
        }
        return solr
    }

    static def paginate(page, per_page, path, opts=null){

        SolrQuery query

        assert page >= 1
        query = new SolrQuery("opinion_text:perjuicio indebido autorizacion de enmienda")
        query.setStart((page-1)*per_page)
        query.setRows(per_page)
        QueryResponse rsp = getSolr().query(query)
        SolrDocumentList sdl = rsp.getResults()
        return sdl
    }

}
