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
package org.wso2.extension.siddhi.store.cassandra.condition;

import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;


import java.util.List;
import java.util.SortedMap;

/**
 * Implementation class of {@link CompiledCondition} corresponding to the Cassandra Event Table.
 * Maintains the conditions returned by the ExpressionVisitor as well as as a set of boolean values for inferring
 * states to be used at runtime.
 */
public class CassandraCompiledCondition implements CompiledCondition {
    private String compiledQuery;
    private SortedMap<Integer, Object> parameters;
    private boolean readOnlyCondition;
    private List<String> storeAttributeNames;

    public CassandraCompiledCondition(String compiledQuery, SortedMap<Integer, Object> parameters,
                                      boolean readOnlyCondition, List<String> storeAttributeNames) {
        this.compiledQuery = compiledQuery;
        this.parameters = parameters;
        this.readOnlyCondition = readOnlyCondition;
        this.storeAttributeNames = storeAttributeNames;
    }

    @Override
    public CompiledCondition cloneCompilation(String key) {
        return new CassandraCompiledCondition(this.compiledQuery, this.parameters, this.readOnlyCondition,
                this.storeAttributeNames);
    }

    /**
     * This method is used to obtain the compiled query
     * @return returns the compiled query
     */
    public String getCompiledQuery() {
        return compiledQuery;
    }

    /**
     * This is used to obtain the String value of the compiled query
     * @return the String value of the compiled query
     */
    public String toString() {
        return getCompiledQuery();
    }

    /**
     * This is used to obtain the sorted map of parameter values according to the value
     * @return returns a map of parameters
     */
    public SortedMap<Integer, Object> getParameters() {
        return parameters;
    }

    /**
     * This method is to get whether this is a readonly condition.
     * Readonly condition means that the Siddhi condition should contain only == (Equal operator) in the
     * condition statement
     * @return returns whether this is a readonly condition.
     */
    public boolean getReadOnlyCondition() {
        return readOnlyCondition;
    }

    /**
     * This method returns the list of attribute names
     *
     */
    public List<String> getStoreAttributeNames() {
        return storeAttributeNames;
    }
}
