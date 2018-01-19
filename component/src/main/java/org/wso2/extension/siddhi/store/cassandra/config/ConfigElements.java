/*
*  Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.wso2.extension.siddhi.store.cassandra.config;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * This class is to map the xml elements with the cassandra-table-config.xml file and to get the
 * cassandra queries when needed.
 */
@XmlRootElement(name = "configElements")
public class ConfigElements {
    private String tableCreateQuery;
    private String tableCheckQuery;
    private String tableValidityQuery;
    private String indexQuery;
    private String recordExistQuery;
    private String recordDeleteQuery;
    private String recordInsertQuery;
    private String recordSelectQuery;
    private String recordSelectNoKeyTable;
    private String recordUpdateQuery;

    public String getTableCreateQuery() {
        return tableCreateQuery;
    }

    public void setTableCreateQuery(String tableCreateQuery) {
        this.tableCreateQuery = tableCreateQuery;
    }

    @XmlElement
    public String getTableCheckQuery() {
        return tableCheckQuery;
    }

    public void setTableCheckQuery(String tableCheckQuery) {
        this.tableCheckQuery = tableCheckQuery;
    }

    @XmlElement
    public String getTableValidityQuery() {
        return tableValidityQuery;
    }

    public void setTableValidityQuery(String tableValidityQuery) {
        this.tableValidityQuery = tableValidityQuery;
    }

    @XmlElement
    public String getIndexQuery() {
        return indexQuery;
    }

    public void setIndexQuery(String indexQuery) {
        this.indexQuery = indexQuery;
    }

    @XmlElement
    public String getRecordExistQuery() {
        return recordExistQuery;
    }

    public void setRecordExistQuery(String recordExistQuery) {
        this.recordExistQuery = recordExistQuery;
    }

    @XmlElement
    public String getRecordDeleteQuery() {
        return recordDeleteQuery;
    }

    public void setRecordDeleteQuery(String recordDeleteQuery) {
        this.recordDeleteQuery = recordDeleteQuery;
    }

    @XmlElement
    public String getRecordInsertQuery() {
        return recordInsertQuery;
    }

    public void setRecordInsertQuery(String recordInsertQuery) {
        this.recordInsertQuery = recordInsertQuery;
    }

    @XmlElement
    public String getRecordSelectQuery() {
        return recordSelectQuery;
    }

    public void setRecordSelectQuery(String recordSelectQuery) {
        this.recordSelectQuery = recordSelectQuery;
    }

    @XmlElement
    public String getRecordSelectNoKeyTable() {
        return recordSelectNoKeyTable;
    }

    public void setRecordSelectNoKeyTable(String recordSelectNoKeyTable) {
        this.recordSelectNoKeyTable = recordSelectNoKeyTable;
    }

    @XmlElement
    public String getRecordUpdateQuery() {
        return recordUpdateQuery;
    }

    public void setRecordUpdateQuery(String recordUpdateQuery) {
        this.recordUpdateQuery = recordUpdateQuery;
    }
}
