package models

case class SolrDocument(override val sourceDoc:Object, override val solrResponse: Object,
   uniqueKey: String
   ) extends Document