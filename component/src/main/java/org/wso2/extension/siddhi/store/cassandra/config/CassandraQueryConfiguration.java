/*
 *  Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.extension.siddhi.store.cassandra.config;

/**
 * This class is to keep the cassandra queries need to do CRUD operations in cassandra
 */
public class CassandraQueryConfiguration {

    public static final String TABLE_CREATE_QUERY = "CREATE TABLE {{KEYSPACE}}.{{TABLE}} ({{COLUMNS}}{{PRIMARY_KEYS}})";
    public static final String TABLE_CHECK_QUERY = "SELECT table_name  FROM system_schema.tables where " +
            "keyspace_name=\'{{KEYSPACE}}\'";
    public static final String TABLE_VALIDITY_QUERY = "SELECT column_name,kind,type  FROM system_schema.columns " +
            "where keyspace_name=\'{{KEYSPACE}}\' AND table_name=\'{{TABLE}}\'";
    public static final String INDEX_CREATE_QUERY = "CREATE INDEX ON {{KEYSPACE}}.{{TABLE}} ({{INDEX}})";
    public static final String RECORD_EXIST_QUERY = "SELECT count(*) FROM {{KEYSPACE}}.{{TABLE}} WHERE " +
            "{{CONDITION}} ALLOW FILTERING";
    public static final String RECORD_DELETE_QUERY = "DELETE FROM {{KEYSPACE}}.{{TABLE}} WHERE {{CONDITION}}";
    public static final String RECORD_INSERT_QUERY = "INSERT INTO {{KEYSPACE}}.{{TABLE}} ({{INSERT_VALUES}}) VALUES " +
            "({{QUESTION_MARKS}}) IF NOT EXISTS";
    public static final String RECORD_SELECT_QUERY = "SELECT {{SELECT_VALUES}} FROM {{KEYSPACE}}.{{TABLE}} " +
            "WHERE {{CONDITION}} ALLOW FILTERING";
    public static final String RECORD_SELECT_NO_KEY_TABLE = "SELECT \"_id\" FROM {{KEYSPACE}}.{{TABLE}} WHERE " +
            "{{CONDITION}} ALLOW FILTERING";
    public static final String RECORD_UPDATE_QUERY = "UPDATE {{KEYSPACE}}.{{TABLE}} SET {{COLUMNS_AND_VALUES}} " +
            "WHERE {{CONDITION}}";

}
