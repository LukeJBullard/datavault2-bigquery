<?php
    /**
     * BigQuery-backed DataVault 2.0 Module for OliveWeb
     * Hub Table backed by Google BigQuery for Storage and a Redis Database for Quick Indexing
     * 
     * @author Luke Bullard
     */

    /**
     * A HubTable that saves Hubs to a Google BigQuery Table
     */
    class BigQueryHubTable extends HubTable
    {
        private $m_projectID;
        private $m_datasetID;
        private $m_tableName;
        private $m_dataFieldName;
        private $m_sourceFieldName;
        private $m_loadDateFieldName;
        private $m_hashKeyFieldName;
        protected $m_bigQuery;
        protected $m_redis;
        protected $m_redisDb;
        protected $m_dbHashKeys;

        /**
         * Constructor for BigQueryTable
         * 
         * @param BigQueryClient $a_bigQueryClient The BigQueryClient object to use when communicating with BigQuery
         * @param String $a_projectID The ID of the project in Google Cloud Platform
         * @param String $a_datasetID The ID of the dataset in the project
         * @param String $a_tableName The name of the BigQuery table to store the Hubs in
         * @param String $a_dataFieldName The name of the column in the DynamoDB that stores the unique identifying data of the Hub
         * @param String $a_sourceFieldName The name of the column in the DynamoDB that stores the initial source of the Hub
         * @param String $a_loadDateFieldName The name of the column in the DynamoDB that stores the initial load date of the Hub
         * @param String $a_hashKeyFieldName The name of the column in the DynamoDB that stores the hash of the Hub (Primary key)
         * @param Redis|Boolean $a_redis The already connected (and database already selected) Redis instance, or Boolean False to not use Redis
         * @param Int $a_redisDb The Redis DB number associated with this table. Defaults to 0
         */
        public function __construct($a_bigQueryClient, $a_projectID, $a_datasetID, $a_tableName, $a_dataFieldName, $a_sourceFieldName, $a_loadDateFieldName, $a_hashKeyFieldName, $a_redis = false, $a_redisDb = 0)
        {
            $this->m_bigQuery = $a_bigQueryClient;
            $this->m_projectID = $a_projectID;
            $this->m_datasetID = $a_datasetID;
            $this->m_tableName = $a_tableName;
            $this->m_dataFieldName = $a_dataFieldName;
            $this->m_sourceFieldName = $a_sourceFieldName;
            $this->m_loadDateFieldName = $a_loadDateFieldName;
            $this->m_hashKeyFieldName = $a_hashKeyFieldName;
            $this->m_redis = $a_redis;
            $this->m_redisDb = $a_redisDb;

            //if redis is not being used
            if ($this->m_redis == false)
            {
                $this->m_dbHashKeys = array();

                //get the hash keys from the db and cache them
                $query = "SELECT %s FROM [%s:%s.%s]";
                $query = sprintf($query,
                                $this->m_hashKeyFieldName,
                                $this->m_projectID, $this->m_datasetID, $this->m_tableName);

                $hashTable = DV2_BigQuery_Utility::runQuery($this->m_bigQuery, $query, true);

                //loop through each returned row and store the hashkey in the list
                foreach ($hashTable as $row)
                {
                    if (isset($row[$this->m_hashKeyFieldName]))
                    {
                        $this->m_dbHashKeys[$row[$this->m_hashKeyFieldName]] = true;
                    }
                }
            }
        }

        /**
         * Saves the hub to bigQuery. If the Hub already exists, skips it.
         * 
         * @param Hub $a_hub The Hub to save to the DynamoDB
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function saveHub($a_hub)
        {
            //if the hub already exists, skip
            if ($this->hubExists($a_hub->getHashKey()))
            {
                return DV2_SUCCESS;
            }

            //create the row structure to insert into bigquery
            $row = array(
                $this->m_hashKeyFieldName => $a_hub->getHashKey(),
                $this->m_dataFieldName => $a_hub->getData(),
                $this->m_sourceFieldName => $a_hub->getSource(),
                $this->m_loadDateFieldName => date("Y-m-d H:i:s")
            );

            //insert the row
            $result = $this->m_bigQuery->dataset($this->m_datasetID)->table($this->m_tableName)->insertRow($row, array("insertId" => $a_hub->getHashKey()));

            //add the hash to the cache/redis if successful
            if ($result->isSuccessful())
            {
                //if redis is not being used
                if ($this->m_redis == false)
                {
                    $this->m_dbHashKeys[$a_hub->getHashKey()] = true;
                    return DV2_SUCCESS;
                }

                //redis is being used
                if (!$this->m_redis->select($this->m_redisDb))
                {
                    return DV2_ERROR;
                }

                return ($this->m_redis->set($a_hub->getHashKey(), "y") ? DV2_SUCCESS : DV2_ERROR);
            }

            //not successful, return error
            return DV2_ERROR;
        }

        /**
         * Retrieves if the hub already exists in the database
         * 
         * @param String $a_hashKey The Hash of the hub to look for
         * @return Boolean|Int Boolean if the hub exists or not, or DV2_ERROR if an error occurred
         */
        public function hubExists($a_hashKey)
        {
            //if redis is not being used
            if ($this->m_redis == false)
            {
                return array_key_exists($a_hashKey, $this->m_dbHashKeys);
            }

            //redis is being used
            if (!$this->m_redis->select($this->m_redisDb))
            {
                return DV2_ERROR;
            }

            return $this->m_redis->exists($a_hashKey) > 0;
        }

        /**
         * Retrieves a hub from the database
         * 
         * @param String $a_hashKey The hash of the Hub to look for
         * @return Hub|Int The retrieved Hub from the database or DV2_ERROR If the Hub could not be retrieved or does not exist
         */
        public function getHub($a_hashKey)
        {
            //if the hub doesn't exist, return error
            if (!$this->hubExists($a_hashKey))
            {
                return DV2_ERROR;
            }

            //get the hub from the db
            $query = "SELECT %s,%s,%s FROM [%s:%s.%s] WHERE %s='%s' LIMIT 1";
            $query = sprintf($query,
                $this->m_loadDateFieldName, $this->m_sourceFieldName, $this->m_dataFieldName,
                $this->m_projectID, $this->m_datasetID, $this->m_tableName,
                $this->m_hashKeyFieldName, $a_hashKey);

            $hubTable = DV2_BigQuery_Utility::runQuery($this->m_bigQuery, $query, true);

            //if no hub was returned, return error
            if (empty($hubTable))
            {
                return DV2_ERROR;
            }
            $data = "";
            $loadDate = "";
            $source = "";

            foreach ($hubTable[0] as $fieldName => $val)
            {
                switch ($fieldName)
                {
                    case $this->m_dataFieldName:
                        $data = $val;
                        break;

                    case $this->m_loadDateFieldName:
                        $loadDate = $val;
                        break;

                    case $this->m_sourceFieldName:
                        $source = $val;
                        break;
                }
            }

            //if the info isn't pulled from the db, return error
            if ($data == "" || $loadDate == "" || $source == "")
            {
                return DV2_ERROR;
            }

            return new Hub(
                $this,
                $source,
                $loadDate,
                $data
            );
        }

        /**
         * Deletes a Hub from the database
         * 
         * @param String $a_hashKey The Hash of the Hub to delete
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function clearHub($a_hashKey)
        {
            //if the hub doesn't exist anyways, return success
            if (!$this->hubExists($a_hashKey))
            {
                return DV2_SUCCESS;
            }

            //delete the hub from the db
            $query = "DELETE FROM [%s:%s.%s] WHERE %s='%s'";
            $query = sprintf($query,
                $this->m_projectID, $this->m_datasetID, $this->m_tableName,
                $this->m_hashKeyFieldName, $a_hashKey);

            $hubTable = DV2_BigQuery_Utility::runQuery($this->m_bigQuery, $query, true);

            //if redis is not being used
            if ($this->m_redis == false)
            {
                unset($this->m_dbHashKeys[$a_hashKey]);
                return DV2_SUCCESS;
            }

            //redis is being used, delete from redis
            if (!$this->m_redis->select($this->m_redisDb))
            {
                return DV2_ERROR;
            }

            $this->m_redis->unlink($a_hashKey);
            return DV2_SUCCESS;
        }
    }
?>