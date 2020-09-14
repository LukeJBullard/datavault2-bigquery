<?php
    /**
     * BigQuery-backed DataVault 2.0 module for OliveWeb
     * A Link Table backed by Google BigQuery for Storage and a Redis Database for Quick Indexing
     * 
     * @author Luke Bullard
     */

    /**
     * A LinkTable that saves to a Google BigQuery Table
     */
    class BigQueryLinkTable extends LinkTable
    {
        private $m_projectID;
        private $m_datasetID;
        private $m_tableName;
        private $m_sourceFieldName;
        private $m_hashKeyFieldName;
        private $m_loadDateFieldName;
        private $m_fieldMap;
        protected $m_bigQuery;
        protected $m_redis;
        protected $m_redisDb;
        protected $m_dbHashKeys;
        

        /**
         * Constructor for BigQueryLinkTable
         * 
         * @param BigQueryClient $a_bigQueryClient The BigQueryClient object to use when communicating with BigQuery
         * @param String $a_projectID The ID of the project in Google Cloud Platform
         * @param String $a_datasetID The ID of the dataset in the project
         * @param String $a_tableName The name of the DynamoDB table to store the Links in
         * @param String $a_sourceFieldName The name of the column in the DynamoDB that stores the initial source of the Link
         * @param String $a_loadDateFieldName The name of the column in the DynamoDB that stores the initial load date of the Link
         * @param String $a_hashKeyFieldName The name of the column in the DynamoDB that stores the hash of the Link (Primary key)
         * @param Array $a_fieldMap An associative array. Each Key is the name of the linked hub, and the Value is the column to
         *              put the data into in the datavault)
         * @param Redis|Boolean $a_redis The already connected (and database already selected) Redis instance, or Boolean False to not use Redis
         * @param Int $a_redisDb The Redis DB number associated with this table. Defaults to 0
         */
        public function __construct($a_bigQueryClient, $a_projectID, $a_datasetID, $a_tableName, $a_sourceFieldName, $a_loadDateFieldName, $a_hashKeyFieldName, $a_fieldMap, $a_redis = false, $a_redisDb = 0)
        {
            $this->m_bigQuery = $a_bigQueryClient;
            $this->m_projectID = $a_projectID;
            $this->m_datasetID = $a_datasetID;
            $this->m_tableName = $a_tableName;
            $this->m_sourceFieldName = $a_sourceFieldName;
            $this->m_loadDateFieldName = $a_loadDateFieldName;
            $this->m_hashKeyFieldName = $a_hashKeyFieldName;
            $this->m_fieldMap = $a_fieldMap;
            $this->m_redis = $a_redis;
            $this->m_redisDb = $a_redisDb;

            //if redis is not being used
            if ($this->m_redis == false)
            {
                $this->m_dbHashKeys = array();

                //get the hash diffs from the db and cache them
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
         * Retrieves if a link exists in the Table
         * 
         * @param String $a_hash The hash of the link to search for.
         * @return Boolean|Int Boolean if the link exists or not, or DV2_ERROR if an error occurred
         */
        public function linkExists($a_hash)
        {
            //if redis is not being used
            if ($this->m_redis == false)
            {
                return array_key_exists($a_hash, $this->m_dbHashKeys);
            }

            //redis is being used
            if (!$this->m_redis->select($this->m_redisDb))
            {
                return DV2_ERROR;
            }
            return $this->m_redis->exists($a_hash) > 0;
        }

        /**
         * Deletes a Link from the table
         * 
         * @param String $a_hash The Hash of the Link to delete
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function clearLink($a_hash)
        {
            //if the link doesn't exist anyways, return success
            if (!$this->linkExists($a_hash))
            {
                return DV2_SUCCESS;
            }

            //delete the link from the db
            $query = "DELETE FROM [%s:%s.%s] WHERE %s='%s'";
            $query = sprintf($query,
                $this->m_projectID, $this->m_datasetID, $this->m_tableName,
                $this->m_hashKeyFieldName, $a_hash);

            $linkTable = DV2_BigQuery_Utility::runQuery($this->m_bigQuery, $query, true);

            //if redis is not being used
            if ($this->m_redis == false)
            {
                unset($this->m_dbHashKeys[$a_hashKey]);
                return DV2_SUCCESS;
            }

            //redis is being used, delete the key from Redis
            if (!$this->m_redis->select($this->m_redisDb))
            {
                return DV2_ERROR;
            }

            //use unlink over del, as unlink is non-blocking
            $this->m_redis->unlink($a_hashKey);
            return DV2_SUCCESS;
        }

        /**
         * Saves a Link to the Table
         * 
         * @param Link $a_link The link to save
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function saveLink($a_link)
        {
            //if the row already exists, return success
            if ($this->linkExists($a_link->getHashKey()))
            {
                return DV2_SUCCESS;
            }

            $row = array(
                $this->m_hashKeyFieldName => $a_link->getHashKey(),
                $this->m_sourceFieldName => $a_link->getSource(),
                $this->m_loadDateFieldName => date("Y-m-d H:i:s")
            );

            $links = $a_link->getLinks();

            //loop through all known linked hubs
            foreach (array_values($this->m_fieldMap) as $field)
            {
                //if this link has a hash for the hub set
                if (isset($links[$field]))
                {
                    //...and the link is valid
                    if ($links[$field] == "")
                    {
                        continue;
                    }

                    //add the hash for the link to the row to insert
                    $row[$field] = $links[$field];
                }
            }
             
            //insert the row into the db
            $result = $this->m_bigQuery->dataset($this->m_datasetID)->table($this->m_tableName)->insertRow($row, array("insertId" => $a_link->getHashKey()));

            //if redis is not being used
            if ($this->m_redis == false)
            {
                $this->m_dbHashKeys[$a_link->getHashKey()] = true;
                return DV2_SUCCESS;
            }

            //redis is being used. store the key in redis
            if (!$this->m_redis->select($this->m_redisDb))
            {
                return DV2_ERROR;
            }

            $this->m_redis->set($a_link->getHashKey(), "y");
            return DV2_SUCCESS;
        }

        /**
         * Retrieves a Link from the Table
         * 
         * @param String $a_hash The Hash of the Link to retrieve
         * @return Link|Int The returned Link or The DV2 status code if the Link could not be retrieved
         */
        public function getLink($a_hash)
        {
            //if the link isn't in the cache, return error
            if (!$this->linkExists($a_hash))
            {
                return DV2_ERROR;
            }

            //get the link from the db
            $args = array(
                $this->m_loadDateFieldName,
                $this->m_sourceFieldName
            );

            //build the query
            $query = "SELECT %s,%s";

            //add each link field into the query
            foreach (array_values($this->m_fieldMap) as $field)
            {
                $query .= ",%s";
                array_push($args, $field);
            }

            //finish building the query string and insert variables via vsprintf
            $query .= " FROM [%s:%s.%s] WHERE %s='%s' LIMIT 1";
            array_push(
                $args,
                $this->m_projectID, $this->m_datasetID, $this->m_tableName,
                $this->m_hashKeyFieldName, $a_hash
            );
            $query = vsprintf($query, $args);

            $linkTable = DV2_BigQuery_Utility::runQuery($this->m_bigQuery, $query, true);

            //if no link was returned, return error
            if (empty($linkTable))
            {
                return DV2_ERROR;
            }

            $links = array();
            $source = "";
            $date = "";

            //go through retrieved data
            foreach ($linkTable[0] as $fieldName => $val)
            {
                switch ($fieldName)
                {
                    case $this->m_sourceFieldName:
                        $source = $val;
                        continue;
                    
                    case $this->m_dateFieldName:
                        $date = $val;
                        continue;
                }

                //not source or date so it must be a link hash column or fixed value
                $links[$fieldName] = $val;
            }

            //if no links were returned or the source or date is invalid, return error
            if (empty($links) || $source == "" || $date == "")
            {
                return DV2_ERROR;
            }

            return new Link(
                $source,
                $date,
                $links
            );
        }
    }
?>