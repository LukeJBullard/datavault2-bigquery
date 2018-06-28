<?php
    /**
     * Data Vault 2.0 Module for OliveWeb
     * Satellite Table backed by Google BigQuery for Storage
     * 
     * @author Luke Bullard
     */

     /**
     * A SatelliteTable that saves Satellites to a BigQuery table
     */
    class BigQuerySatelliteTable extends SatelliteTable
    {
        private $m_projectID;
        private $m_datasetID;
        private $m_fieldMap;
        private $m_tableName;
        private $m_sourceFieldName;
        private $m_dateFieldName;
        private $m_hashDiffFieldName;
        private $m_hubHashFieldName;
        protected $m_bigQuery;
        protected $m_dbHashKeys;

        /**
         * Constructor for BigQuerySatelliteTable
         * 
         * @param BigQueryClient $a_bigQueryClient The BigQueryClient object to use when communicating with BigQuery
         * @param String $a_projectID The ID of the project in Google Cloud Platform
         * @param String $a_datasetID The ID of the dataset in the project
         * @param String $a_tableName The name of the BigQuery table to use.
         * @param String $a_sourceFieldName The column in the database to put the Satellite's Data Source information.
         * @param String $a_dateFieldName The column in the database to put the Date that the Satellite was loaded.
         * @param String $a_hashDiffFieldName The column in the database to put the Hash of the Satellite.
         * @param String $a_hubHashFieldName The column in the database to put the hash of the Hub the Satellite is linked to.
         * @param Array $a_fieldMap An associative array. Each Key is the name of the Satellite's Data, and the Value is
         *                  the column to put the data into in datavault)
         */
        public function __construct($a_bigQueryClient, $a_projectID, $a_datasetID, $a_tableName, $a_sourceFieldName, $a_dateFieldName,
                                    $a_hashDiffFieldName, $a_hubHashFieldName, $a_fieldMap=array())
        {
            $this->m_projectID = $a_projectID;
            $this->m_datasetID = $a_datasetID;
            $this->m_tableName = $a_tableName;
            $this->m_fieldMap = $a_fieldMap;
            $this->m_sourceFieldName = $a_sourceFieldName;
            $this->m_dateFieldName = $a_dateFieldName;
            $this->m_hashDiffFieldName = $a_hashDiffFieldName;
            $this->m_hubHashFieldName = $a_hubHashFieldName;
            $this->m_bigQuery = $a_bigQueryClient;
            $this->m_dbHashKeys = array();

            //get the hash diffs from the db and cache them
            $query = "SELECT %s,%s FROM [%s:%s.%s]";
            $query = sprintf($query,
                            $this->m_hashDiffFieldName, $this->m_hubHashFieldName,
                            $this->m_projectID, $this->m_datasetID, $this->m_tableName);

            $hashTable = DV2_BigQuery_Utility::runQuery($this->m_bigQuery, $query, true);

            //loop through each returned row and store the hashkey in the list
            foreach ($hashTable as $row)
            {
                if (isset($row[$this->m_hashDiffFieldName], $row[$this->m_hubHashFieldName]))
                {
                    //if the hub hash array for this hash diff has not been created yet
                    if (!isset($this->m_dbHashKeys[$row[$this->m_hashDiffFieldName]]))
                    {
                        $this->m_dbHashKeys[$row[$this->m_hashDiffFieldName]] = array();
                    }
                    array_push($this->m_dbHashKeys[$row[$this->m_hashDiffFieldName]], $row[$this->m_hubHashFieldName]);
                }
            }
        }

        /**
         * Returns if the specified Satellite exists in the Table
         * 
         * @param String $a_hashDiff The Hash Diff of the Satellite.
         * @param String $a_hubHash The Hash of the Hub the Satellite is under. (Optional- if omitted, will search for the first Satellite with the hash diff)
         * @return Boolean If the Satellite exists.
         */
        public function satelliteExists($a_hashDiff, $a_hubHash="")
        {
            if ($a_hubHash == "")
            {
                return array_key_exists($a_hashDiff, $this->m_dbHashKeys);
            }

            if (array_key_exists($a_hashDiff, $this->m_dbHashKeys))
            {
                return in_array($a_hubHash, $this->m_dbHashKeys[$a_hashDiff]);
            }
            
            return false;
        }

        /**
         * Retrieves a Satellite from the Table
         * @param String $a_hashDiff The Hash Diff of the Satellite to retrieve
         * @param String $a_hubHash The Hash of the Hub the Satellite is under (Optional- if omitted, will return the first Satellite with the hash diff)
         * @return Satellite The Satellite retrieved from the Table
         * @return Int DV2_ERROR If the Satellite was not found or could not be loaded
         */
        public function getSatellite($a_hashDiff, $a_hubHash="")
        {
            //if the satellite isn't in the cache, return error
            if (!$this->satelliteExists($a_hashDiff, $a_hubHash))
            {
                return DV2_ERROR;
            }

            //get the satellite from the db
            $args = array(
                $this->m_loadDateFieldName,
                $this->m_sourceFieldName,
                $this->m_hubHashFieldName
            );

            //build the query
            $query = "SELECT %s,%s,%s";

            //add each link field into the query
            foreach (array_values($this->m_fieldMap) as $field)
            {
                $query .= ",%s";
                array_push($args, $field);
            }

            //finish building the query string and insert variables via vsprintf
            $query .= " FROM [%s:%s.%s] WHERE %s='%s'";

            array_push(
                $args,
                $this->m_projectID, $this->m_datasetID, $this->m_tableName,
                $this->m_hashDiffFieldName, $a_hashDiff
            );

            if ($a_hubHash != "")
            {
                $query .= " AND %s='%s'";
                array_push($args, $this->m_hubHashFieldName, $a_hubHash);
            }

            $query .= " LIMIT 1";

            $query = vsprintf($query, $args);

            $satTable = DV2_BigQuery_Utility::runQuery($this->m_bigQuery, $query, true);

            //if no satellite was returned, return error
            if (empty($satTable))
            {
                return DV2_ERROR;
            }

            $data = array();
            $source = "";
            $date = "";
            $hubHash = "";

            foreach ($satTable[0] as $fieldName => $val)
            {
                switch ($fieldName)
                {
                    case $this->m_sourceFieldName:
                        $source = $val;
                        continue;
                    
                    case $this->m_dateFieldName:
                        $date = $val;
                        continue;

                    case $this->m_hubHashFieldName:
                        $hubHash = $val;
                        continue;
                }
                $data[$fieldName] = $val;
            }

            //if the data retrieved is invalid, return error
            if (empty($data) || $source == "" || $date == "" || $hubHash == "")
            {
                return DV2_ERROR;
            }

            return new Satellite(
                $source,
                $date,
                $hubHash,
                $data
            );
        }

        /**
         * Deletes the Satellite from the Table
         * 
         * @param String $a_hashDiff The hash of the Satellite to delete
         * @param String $a_hubHash The Hash of the Hub the Satellite is under (Optional- if omitted, will clear the first Satellite with the hash diff)
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function clearSatellite($a_hashDiff, $a_hubHash="")
        {
            //if the satellite doesn't exist anyways, return success
            if (!$this->satelliteExists($a_hash, $a_hubHash))
            {
                return DV2_SUCCESS;
            }

            //delete the satellite from the db
            $query = "DELETE FROM [%s:%s.%s] WHERE %s='%s'";
            $args = array(
                $this->m_projectID, $this->m_datasetID, $this->m_tableName,
                $this->m_hashDiffFieldName, $a_hash
            );

            if ($a_hubHash != "")
            {
                $query .= " AND %s='%s'";
                array_push($args, $this->m_hubHashFieldName, $a_hubHash);
            }

            $query = vsprintf($query, $args);

            $satelliteTable = DV2_BigQuery_Utility::runQuery($this->m_bigQuery, $query, true);

            unset($this->m_dbHashKeys[$a_hash]);

            return DV2_SUCCESS;
        }

        /**
         * Saves a Satellite in the Table
         * 
         * @param Satellite $a_satellite
         * @return Int DV2_SUCCESS or DV2_ERROR
         */
        public function saveSatellite($a_satellite)
        {
            //if the satellite already exists, return success
            if ($this->satelliteExists($a_satellite->getHashDiff(), $a_satellite->getHubHash()))
            {
                return DV2_SUCCESS;
            }

            $row = array(
                $this->m_hashDiffFieldName => $a_satellite->getHashDiff(),
                $this->m_sourceFieldName => $a_satellite->getSource(),
                $this->m_hubHashFieldName => $a_satellite->getHubHash(),
                $this->m_dateFieldName => date("Y-m-d H:i:s")
            );

            $data = $a_satellite->getData();

            //loop through all known data columns
            foreach ($this->m_fieldMap as $codeField => $dbField)
            {
                //if this satellite has data for the column
                if (isset($data[$codeField]))
                {
                    //add the data to the row
                    $row[$dbField] = $data[$codeField];
                }
            }
             
            //insert the satellite into the db
            $result = $this->m_bigQuery->dataset($this->m_datasetID)->table($this->m_tableName)->insertRow($row, array("insertId" => $a_satellite->getHashDiff() . $a_satellite->getHubHash()));
            
            //add the satellite to the cache
            //if the hub array for this hash diff is not already added to the cache, add it
            if (!isset($this->m_dbHashKeys[$a_satellite->getHashDiff()]))
            {
                $this->m_dbHashKeys[$a_satellite->getHashDiff()] = array();
            }
            array_push($this->m_dbHashKeys[$a_satellite->getHashDiff()], $a_satellite->getHubHash());

            return DV2_SUCCESS;
        }
    }
?>