<?php
    /**
     * BigQuery addon for DataVault 2.0 Module for OliveWeb
     * 
     * @author Luke Bullard
     */

    //make sure we are included securely
    if (!defined("INPROCESS")) { header("HTTP/1.0 403 Forbidden"); exit(0); }

    /**
     * The BigQuery Addon OliveWeb Module
     */
    class MOD_dv2bigquery
    {
        public function __construct()
        {
            //load datavault2 module
            Modules::getInstance()['datavault2'];

            //load mod src files
            require_once("src/Utility.php");
            require_once("src/BigQueryHubTable.php");
            require_once("src/BigQueryLinkTable.php");
            require_once("src/BigQuerySatelliteTable.php");
        }
    }
?>