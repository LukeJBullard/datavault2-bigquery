<?php
    /**
     * BigQuery-backed DataVault 2.0 Module for OliveWeb
     * Utility Functions
     * 
     * @author Luke Bullard
     */

    class DV2_BigQuery_Utility
    {
        /**
         * Runs a BigQuery query and retrieves the results
         * 
         * @param BigQueryClient $a_bigQueryClient The BigQueryClient to perform the query against
         * @param String $a_query The SQL query to run
         * @param Boolean $a_useLegacySql True if legacy SQL syntax should be used
         * @return Array[] A 2-dimensional array of the results returned from the database. The inner arrays are associative,
         *                  with the key being the column and the value being the row.
         */
        public static function runQuery($a_bigQueryClient, $a_query, $a_useLegacySql = false)
        {
            $jobConfig = $a_bigQueryClient->query($a_query)->useLegacySql($a_useLegacySql);
            $queryResults = $a_bigQueryClient->runQuery($jobConfig);
            
            return iterator_to_array($queryResults);
        }
    }
?>