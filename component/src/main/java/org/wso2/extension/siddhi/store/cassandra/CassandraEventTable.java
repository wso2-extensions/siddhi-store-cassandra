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

package org.wso2.extension.siddhi.store.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.log4j.Logger;

import org.wso2.extension.siddhi.store.cassandra.condition.CassandraCompiledCondition;
import org.wso2.extension.siddhi.store.cassandra.condition.CassandraConditionVisitor;
import org.wso2.extension.siddhi.store.cassandra.config.ConfigElements;
import org.wso2.extension.siddhi.store.cassandra.exception.CassandraTableException;
import org.wso2.extension.siddhi.store.cassandra.iterator.CassandraIterator;
import org.wso2.extension.siddhi.store.cassandra.util.CassandraTableUtils;
import org.wso2.extension.siddhi.store.cassandra.util.Constant;
import org.wso2.extension.siddhi.store.cassandra.util.TableMeta;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.exception.ConnectionUnavailableException;
import org.wso2.siddhi.core.table.record.AbstractRecordTable;
import org.wso2.siddhi.core.table.record.ExpressionBuilder;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.collection.operator.CompiledCondition;
import org.wso2.siddhi.core.util.collection.operator.CompiledExpression;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.annotation.Annotation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.TableDefinition;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.ANNOTATION_ELEMENT_KEY_SPACE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.ANNOTATION_ELEMENT_TABLE_NAME;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.ANNOTATION_HOST;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.ANNOTATION_PASSWORD;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.ANNOTATION_USER_NAME;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CQL_AND;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CQL_FILTERING;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CQL_PRIMARY_KEY_DEF;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CQL_WHERE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.DEFAULT_KEY_SPACE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.EQUALS;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.OPEN_PARENTHESIS;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_COLUMNS;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.
        PLACEHOLDER_COLUMNS_AND_VALUES;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_CONDITION;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_INDEX;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_INSERT_VALUES;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_KEYSPACE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_PRIMARY_KEYS;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_QUESTION_MARKS;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_SELECT_VALUES;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.PLACEHOLDER_TABLE;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.QUESTION_MARK;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.SEPARATOR;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.WHITESPACE;

import static org.wso2.siddhi.core.util.SiddhiConstants.ANNOTATION_STORE;


/**
 * Class representing the Cassandra Event Table implementation.
 */
@Extension(
        name = "cassandra",
        namespace = "store",
        description = "This extension assigns data sources and connection instructions to event tables. It also " +
                "implements read-write operations on connected datasource.",
        parameters = {
                @Parameter(name = "cassandra.host",
                        description = "Host that is used to get connected in to the cassandra keyspace.",
                        type = {DataType.STRING},
                        defaultValue = "localhost"),
                @Parameter(name = "table.name",
                        description = "The name with which the event table should be persisted in the store. If no " +
                                "name is specified via this parameter, the event table is persisted with the same " +
                                "name as the Siddhi table.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "The table name defined in the Siddhi Application query."),
                @Parameter(name = "keyspace",
                        description = "User need to give the keyspace that the data is persisted. " +
                                "It is ven by the keyspace parameter",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "'stockTable'"),
                @Parameter(name = "username",
                        description = "Through user name user can specify the relevent username " +
                                "that is used to log in to the cassandra keyspace .",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "''"),
                @Parameter(name = "password",
                        description = "Through password user can specify the relevent password " +
                                "that is used to log in to the cassandra keyspace .",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "''"),
        },
        examples = {
                @Example(
                        syntax = "define stream StockStream (symbol string, price float, volume long); \n" +
                                "@Store(type=\"cassandra\", table.name=\"StockTable\",keyspace=\"AnalyticsFamily\"," +
                                "username=\"cassandra\",password=\"cassandra\",cassandra.host=\"localhost\")" +
                                "@IndexBy(\"volume\")" +
                                "@PrimaryKey(\"symbol\")" +
                                "define table StockTable (symbol string, price float, volume long); ",
                        description = "This definition creates an event table named `StockTable` with a column " +
                                "family `StockCF` on the Cassandra instance if it does not already exist (with 3 " +
                                "attributes named `symbol`, `price`, and `volume` of the `string`, " +
                                "`float` and `long` types respectively). The connection is made as specified by the " +
                                "parameters configured for the '@Store' annotation. The `symbol` attribute is " +
                                "considered a unique field, and the values for this attribute are the " +
                                "Cassandra row IDs."
                )
        }
)
public class CassandraEventTable extends AbstractRecordTable {

    private Session session;
    private List<Attribute> schema;
    private List<Attribute> primaryKeys;
    private Annotation storeAnnotation;
    private String tableName;
    private String keyspace;
    private String host;
    private String addDataQuerySt;
    private List<Integer> objectAttributes;  // Used for object data insertion.
    private boolean noKeys;
    private String selectQuery;
    private String updateQuery;
    private Annotation indexAnnotation;
    private boolean noKeyTable;
    private Map<String, String> persistedKeyColumns;
    private ConfigElements configElements;

    private static final Logger LOG = Logger.getLogger(CassandraEventTable.class);

    /**
     * Initializing the Record Table
     *
     * @param tableDefinition definintion of the table with annotations if any
     * @param configReader    this hold the {@link AbstractRecordTable} configuration reader.
     */
    @Override
    protected void init(TableDefinition tableDefinition, ConfigReader configReader) {

        this.schema = tableDefinition.getAttributeList();
        this.storeAnnotation = AnnotationHelper.getAnnotation(ANNOTATION_STORE, tableDefinition.getAnnotations());
        Annotation primaryKeyAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_PRIMARY_KEY,
                tableDefinition.getAnnotations());
        this.indexAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_INDEX_BY,
                tableDefinition.getAnnotations());
        String tableName = storeAnnotation.getElement(ANNOTATION_ELEMENT_TABLE_NAME);
        String keyspace = storeAnnotation.getElement(ANNOTATION_ELEMENT_KEY_SPACE);
        LOG.info(tableName);
        this.tableName = CassandraTableUtils.isEmpty(tableName) ? tableDefinition.getId() : tableName;
        this.host = storeAnnotation.getElement(ANNOTATION_HOST);
        this.keyspace = CassandraTableUtils.isEmpty(keyspace) ? DEFAULT_KEY_SPACE : keyspace;
        LOG.info("Starting");
        LOG.info(schema);
        LOG.info(primaryKeyAnnotation);
        LOG.info(storeAnnotation);
        LOG.info(host);
        LOG.info("table name :" + this.tableName);
        LOG.info("keyspace name :" + this.keyspace);
        LOG.info("Ending");
        LOG.info(QueryBuilder.delete().column("abs"));

        // loading cassandra config file
        try {
            readConfigFile();
        } catch (JAXBException e) {
            throw new CassandraTableException("Could not find the configuration file. Please insert the " +
                    "cassandra-table-config.xml in the resources folder", e);
        }

        Attribute pk;
        if (primaryKeyAnnotation == null) {
            this.noKeys = true;
            this.primaryKeys = new ArrayList<>();
            pk = new Attribute("\"_id\"", Attribute.Type.STRING);
            this.primaryKeys.add(pk);
        } else {
            this.primaryKeys = CassandraTableUtils.initPrimaryKeys(this.schema, primaryKeyAnnotation);
        }
        LOG.info(primaryKeys);
        LOG.info(schema);

        //Generating the value statement and the question marks to be used in prepared
        // statement and detecting object attributes
        int i = 0;
        StringBuilder insertValStatement = new StringBuilder();
        StringBuilder questionMarks = new StringBuilder();
        this.objectAttributes = new ArrayList<>();
        for (Attribute a : schema) {
            insertValStatement.append(a.getName());
            questionMarks.append(QUESTION_MARK);
            //building the insert value statement
            if (i != schema.size() - 1) {
                insertValStatement.append(SEPARATOR);
                questionMarks.append(SEPARATOR);
            }
            //keeping the object attributes in a separate array
            if (schema.get(i).getType() == Attribute.Type.OBJECT) {
                this.objectAttributes.add(i);
            }
            i++;
        }

        //initialising the select value statement
        String selectValStatement = insertValStatement.toString();

        if (this.noKeys) {
            questionMarks.append(SEPARATOR).append(QUESTION_MARK);
            insertValStatement.append(SEPARATOR).append("\"_id\"");
        }

        //query initialization to add data to the cassandra keyspace
        this.addDataQuerySt = this.configElements.getRecordInsertQuery().replace(PLACEHOLDER_KEYSPACE, this.keyspace).
                replace(PLACEHOLDER_TABLE, this.tableName).replace(PLACEHOLDER_INSERT_VALUES, insertValStatement).
                replace(PLACEHOLDER_QUESTION_MARKS, questionMarks);
        LOG.info(addDataQuerySt);

        //Query to search data
        this.selectQuery = this.configElements.getRecordSelectQuery().
                replace(PLACEHOLDER_SELECT_VALUES, selectValStatement).
                replace(PLACEHOLDER_KEYSPACE, this.keyspace).replace(PLACEHOLDER_TABLE, this.tableName);
    }

    /**
     * Add records to the Table
     *
     * @param records records that need to be added to the table, each Object[] represent a record and it will match
     *                the attributes of the Table Definition.
     */
    @Override
    protected void add(List<Object[]> records) throws ConnectionUnavailableException {
        PreparedStatement prepared = this.session.prepare(this.addDataQuerySt);
        for (Object record[] : records) {
            if (this.objectAttributes.size() != 0) {
                for (int columnNo : this.objectAttributes) {
                    Object oldData = record[columnNo];
                    try {
                        record[columnNo] = resolveObjectData(oldData);
                    } catch (IOException ex) {
                        throw new CassandraTableException("Error in object insertion ensure that the objects " +
                                "are serializable.", ex);
                    }
                }
            }
            addData(record, prepared);
        }
    }

    private Object resolveObjectData(Object cellData) throws ConnectionUnavailableException, IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos);
        out.writeObject(cellData);
        out.flush();
        byte[] dataToBytes = bos.toByteArray();
        bos.close();
        return ByteBuffer.wrap(dataToBytes);
    }

    /**
     * This method will be used to inset data to the cassandra keyspace
     * . @param records records that need to be added to the table, each Object[] represent a record and it will match
     * the attributes of the Table Definition passed by the add method in CassandraEventTable.
     */
    private void addData(Object[] record, PreparedStatement prepared) throws ConnectionUnavailableException {
        //Need to decide whether this table has a primary key. If there is a primary key
        // then the primary key annotation should n`t be null
        BoundStatement bound;
        if (this.noKeyTable) {
            ArrayList<Object> pkRecords = new ArrayList<>(Arrays.asList(record));
            pkRecords.add(CassandraTableUtils.generatePrimaryKeyValue());
            bound = prepared.bind(pkRecords.toArray());
            this.session.execute(bound);
        } else {
            bound = prepared.bind(record);
            this.session.execute(bound);
        }
    }

    @Override
    protected RecordIterator<Object[]> find(Map<String, Object> findConditionParameterMap,
                                            CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        CassandraCompiledCondition cassandraCompiledCondition = (CassandraCompiledCondition) compiledCondition;
        String compiledQuery = cassandraCompiledCondition.getCompiledQuery();
        //This array consists of values to be passed to the prepared statement
        String finalSearchQuery;
        PreparedStatement prepared;
        ResultSet result;
        switch (compiledQuery) {
            case QUESTION_MARK:
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignore the condition resolver in 'find()' method for compile " +
                            "condition: '" + QUESTION_MARK + "'");
                }
                compiledQuery = compiledQuery.replace(QUESTION_MARK, "");
                finalSearchQuery = this.selectQuery.replace(PLACEHOLDER_CONDITION, compiledQuery);
                LOG.info(finalSearchQuery);
                result = this.session.execute(finalSearchQuery);
                break;
            case "":
                finalSearchQuery = this.selectQuery.replace(PLACEHOLDER_CONDITION, "").
                        replace(CQL_WHERE, "").replace(CQL_FILTERING, "");
                LOG.info(finalSearchQuery);
                result = this.session.execute(finalSearchQuery);
                break;
            default:
                finalSearchQuery = this.selectQuery.replace(PLACEHOLDER_CONDITION, compiledQuery);
                LOG.info(finalSearchQuery);
                prepared = this.session.prepare(finalSearchQuery);
                Object[] argSet = constructArgSet(compiledCondition, findConditionParameterMap);
                BoundStatement bound = prepared.bind(argSet);
                result = this.session.execute(bound);
        }
        return new CassandraIterator(result.iterator(), this.schema);
    }

    /**
     * Find arguments to matching the compiled condition
     *
     * @param compiledCondition map of matching StreamVariable Ids and their values
     *                                  corresponding to the compiled condition
     * @param conditionParameterMap         the compiledCondition against which records should be matched
     * @return Object[] of matching arguments
     */
    private Object[] constructArgSet(CompiledCondition compiledCondition,
                                     Map<String, Object> conditionParameterMap) {
        CassandraCompiledCondition cassandraCompiledCondition = (CassandraCompiledCondition) compiledCondition;
        SortedMap<Integer, Object> compiledParameters = cassandraCompiledCondition.getParameters();
        Object[] argSet = new Object[compiledParameters.size()];
        int i = 0;
        for (SortedMap.Entry<Integer, Object> entry : compiledParameters.entrySet()) {
            Object parameter = entry.getValue();
            if (parameter instanceof Constant) {
                // if the value is a constant
                Constant constant = (Constant) parameter;
                argSet[i] = constant.getValue();
            } else {
                // if the value is an attribute
                Attribute variable = (Attribute) parameter;
                String attributeName = variable.getName();
                // checks whether attributeName in the compiledParameters are equal to the attribute name in the
                // conditionParameterMap, if those two are equal then added to the argument set
                for (Map.Entry<String, Object> userEntry : conditionParameterMap.entrySet()) {
                    if (userEntry.getKey().toLowerCase(Locale.ENGLISH).equals(
                            attributeName.toLowerCase(Locale.ENGLISH))) {
                        argSet[i] = userEntry.getValue();
                    }
                }
            }
            i++;
        }
        return argSet;
    }

    /**
     * Check if matching record exist
     *
     * @param containsConditionParameterMap map of matching StreamVariable Ids and their values corresponding to the
     *                                      compiled condition
     * @param compiledCondition             the compiledCondition against which records should be matched
     * @return if matching record found or not
     */
    @Override
    protected boolean contains(Map<String, Object> containsConditionParameterMap,
                               CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        Object[] argSet = constructArgSet(compiledCondition, containsConditionParameterMap);
        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();
        String cql = this.configElements.getRecordExistQuery().replace(PLACEHOLDER_KEYSPACE, this.keyspace).
                replace(PLACEHOLDER_TABLE, this.tableName).replace(PLACEHOLDER_CONDITION, compiledQuery);
        LOG.info(cql);
        PreparedStatement prepared = session.prepare(cql);
        BoundStatement boundStatement = prepared.bind(argSet);
        ResultSet rs = this.session.execute(boundStatement);
        if (rs.one().getLong("count") > 0) {
            LOG.info("true");
            return true;
        } else {
            LOG.info("false");
            return false;
        }
    }

    @Override
    protected void delete(List<Map<String, Object>> deleteConditionParameterMaps,
                          CompiledCondition compiledCondition) throws ConnectionUnavailableException {
        String deleteQuery = this.configElements.getRecordDeleteQuery().replace(PLACEHOLDER_KEYSPACE, this.keyspace).
                replace(PLACEHOLDER_TABLE, this.tableName).replace(PLACEHOLDER_CONDITION,
                ((CassandraCompiledCondition) compiledCondition).getCompiledQuery());
        for (Map<String, Object> deleteConditionMap : deleteConditionParameterMaps) {
            if (containsAllPrimaryKeys(deleteConditionMap) && !(this.noKeyTable) &&
                    ((CassandraCompiledCondition) compiledCondition).getReadOnlyCondition()) {
                deleteSingleRow(deleteConditionMap, compiledCondition, deleteQuery);
            } else if (this.noKeyTable) {
                // need to find the key values with the column name _id
                ArrayList<String> ids = findAllIDs(compiledCondition, deleteConditionMap);
                executeAsBatch_noId_delete(ids);
            } else {
                // need to find the key values defined by user in table defining
                ArrayList<Object[]> ids = findAllUserDefinedIDs(compiledCondition, deleteConditionMap);
                executeAsBatch_nonPrime_delete(ids);
            }
        }
    }

    /**
     * This used to find all ids (Primary Key) to update or delete a row in case where
     * user has not defined the primary key in the data insertion state.
     */
    private ArrayList<String> findAllIDs(CompiledCondition compiledCondition,
                                         Map<String, Object> conditionParameterMap) {
        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();
        String finalSearchQuery = this.configElements.getRecordSelectNoKeyTable().
                replace(PLACEHOLDER_KEYSPACE, this.keyspace).replace(PLACEHOLDER_TABLE, this.tableName).
                replace(PLACEHOLDER_CONDITION, compiledQuery);
        LOG.info(finalSearchQuery);
        PreparedStatement prepared = this.session.prepare(finalSearchQuery);
        Object[] argSet = constructArgSet(compiledCondition, conditionParameterMap);
        BoundStatement bound = prepared.bind(argSet);
        ArrayList<String> ids = new ArrayList<>();
        ResultSet result = this.session.execute(bound);
        for (Row row : result) {
            ids.add(row.getString("\"_id\""));
        }
        return ids;
    }

    /**
     * This used to find all ids (Primary Key) to update or delete a row in case where
     * user has not defined the primary key in the the condition.
     */
    private ArrayList<Object[]> findAllUserDefinedIDs(CompiledCondition compiledCondition,
                                                      Map<String, Object> conditionParameterMap) {
        // constructs the values to be extracted fro the relevent table
        // eg - select val1,val2,val3
        StringBuilder keyValueSelector = new StringBuilder();
        int i = 0;
        for (Map.Entry<String, String> persistedColumn : persistedKeyColumns.entrySet()) {
            keyValueSelector.append(persistedColumn.getKey());
            if (i != this.persistedKeyColumns.size() - 1) {
                keyValueSelector.append(SEPARATOR);
            }
            i++;
        }
        LOG.info(keyValueSelector.toString());
        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();
        String finalSearchQuery = this.configElements.getRecordSelectQuery().
                replace(PLACEHOLDER_SELECT_VALUES, keyValueSelector.toString()).
                replace(PLACEHOLDER_KEYSPACE, this.keyspace).replace(PLACEHOLDER_TABLE, this.tableName).
                replace(PLACEHOLDER_CONDITION, compiledQuery);
        PreparedStatement prepared = this.session.prepare(finalSearchQuery);
        Object[] argSet = constructArgSet(compiledCondition, conditionParameterMap);
        BoundStatement bound = prepared.bind(argSet);
        ArrayList<Object[]> ids = new ArrayList<>();
        ResultSet result = session.execute(bound);
        for (Row row : result) {
            Object[] rowKey = new Object[this.persistedKeyColumns.size()];
            int rowNo = 0;
            for (Map.Entry<String, String> persistedColumn : this.persistedKeyColumns.entrySet()) {
                rowKey[rowNo] = row.getObject(persistedColumn.getKey());
                rowNo++;
            }
            ids.add(rowKey);
        }
        return ids;
    }

    /**
     * This creates the prepared statement that is used in a table where a
     * primary key is not defined by the user as a batch and also the batch is executed.
     */
    private void executeAsBatch_noId_delete(ArrayList<String> ids) {
        BatchStatement batchStatement = new BatchStatement();
        String condition = "\"_id\"" + EQUALS + QUESTION_MARK;
        String deleteQuery = this.configElements.getRecordDeleteQuery().replace(PLACEHOLDER_KEYSPACE, this.keyspace).
                replace(PLACEHOLDER_TABLE, this.tableName).replace(PLACEHOLDER_CONDITION, condition);
        PreparedStatement preparedStatement = session.prepare(deleteQuery);
        for (Object id : ids) {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            boundStatement.bind(id);
            batchStatement.add(boundStatement);
        }
        this.session.execute(batchStatement);
    }

    /**
     * This creates the prepared statement (to delete the table) that is used in a table where a
     * primary key is not defined at the condition by the user as a batch and also the batch is executed.
     */
    private void executeAsBatch_nonPrime_delete(ArrayList<Object[]> ids) {
        int i = 0;
        StringBuilder condition = new StringBuilder();
        // building the condition statement
        for (Map.Entry<String, String> persistedColumn : this.persistedKeyColumns.entrySet()) {
            condition.append(persistedColumn.getKey()).append(EQUALS).append(QUESTION_MARK);
            if (i != this.persistedKeyColumns.size() - 1) {
                condition.append(WHITESPACE).append(CQL_AND).append(WHITESPACE);
            }
            i++;
        }
        String deleteQuery = this.configElements.getRecordDeleteQuery().replace(PLACEHOLDER_KEYSPACE, this.keyspace).
                replace(PLACEHOLDER_TABLE, this.tableName).replace(PLACEHOLDER_CONDITION, condition.toString());
        LOG.info(deleteQuery);
        BatchStatement batchStatement = new BatchStatement();
        PreparedStatement preparedStatement = this.session.prepare(deleteQuery);
        for (Object[] id : ids) {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            boundStatement.bind(id);
            batchStatement.add(boundStatement);
        }
        this.session.execute(batchStatement);
    }

    /**
     * This creates the prepared statement (to update the table) that is used in a table where a
     * primary key is not defined at the condition by the user as a batch and also the batch is executed.
     */
    private void executeAsBatch_nonPrime_update(ArrayList<Object[]> ids, Map<String, Object> updateParameterMap) {
        // keys that are in the condition as well as the set parameters
        // these should be removed
        ArrayList<String> keys = new ArrayList<>();
        for (Map.Entry<String, Object> parameter : updateParameterMap.entrySet()) {
            if (this.persistedKeyColumns.containsKey(parameter.getKey())) {
                keys.add(parameter.getKey());
            }
        }
        //Since cassandra cannot update a primary key column we need to remove the primary key values sent
        for (String key : keys) {
            updateParameterMap.remove(key);
        }

        BatchStatement batchStatement = new BatchStatement();
        ArrayList<Object> setValues = buildUpdateSetStatement(updateParameterMap);
        PreparedStatement preparedStatement = this.session.prepare(this.updateQuery);
        for (Object[] id : ids) {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            setValues.addAll(new ArrayList<>(Arrays.asList(id)));
            boundStatement.bind(setValues.toArray());
            setValues.removeAll(new ArrayList<>(Arrays.asList(id)));
            batchStatement.add(boundStatement);
        }
        this.session.execute(batchStatement);
    }

    /**
     * This creates the prepared statement that is used in a table where a
     * primary key is not defined by the user as a batch and also the batch is executed.
     */
    private void executeAsBatch_noId_update(ArrayList<String> ids, Map<String, Object> updateParameterMap) {
        BatchStatement batchStatement = new BatchStatement();
        ArrayList<Object> setValues = buildCQLStatement_Update(updateParameterMap);
        String condition = "\"_id\"" + EQUALS + QUESTION_MARK;
        String finalUpdateQuery = this.updateQuery.replace(PLACEHOLDER_CONDITION, condition);
        PreparedStatement preparedStatement = session.prepare(finalUpdateQuery);
        for (Object id : ids) {
            BoundStatement boundStatement = new BoundStatement(preparedStatement);
            setValues.add(id);
            boundStatement.bind(setValues.toArray());
            setValues.remove(id);
            batchStatement.add(boundStatement);
        }
        this.session.execute(batchStatement);
    }

    /**
     * This is used to delete a single row entry that is matched with the primary key
     */
    private void deleteSingleRow(Map<String, Object> deleteConditionParameterMap,
                                 CompiledCondition compiledCondition, String deleteQuery) {
        Object argSet[] = constructArgSet(compiledCondition, deleteConditionParameterMap);
        //InvalidQueryException may take place if the defined if the condition doeas not have keys :Throw exception.
        PreparedStatement prepared = this.session.prepare(deleteQuery);
        LOG.info(deleteQuery + " " + argSet[0]);
        BoundStatement bound = prepared.bind(argSet);
        this.session.execute(bound);
    }

    @Override
    protected void update(CompiledCondition updateCondition, List<Map<String, Object>> updateConditionParameterMaps,
                          Map<String, CompiledExpression> updateSetExpressions, List<Map<String,
            Object>> updateSetParameterMaps) throws ConnectionUnavailableException {

        int i = 0;
        for (Map<String, Object> updateSetParameterMap : updateSetParameterMaps) {
            // When the user has define the primary key
            if (containsAllPrimaryKeys(updateConditionParameterMaps.get(i)) && !(noKeyTable) &&
                    ((CassandraCompiledCondition) updateCondition).getReadOnlyCondition()) {
                // if there is a match to the provided key is found update is possible
                if (contains(updateConditionParameterMaps.get(i), updateCondition)) {
                    updateSingleRow(updateSetParameterMap, updateCondition, updateConditionParameterMaps.get(i));
                } else {
                    throw new CassandraTableException("Row does not exists with the provided keys.. " +
                            "Try to update with existing keys. Update failed. ");
                }

            } else if (this.noKeyTable) {
                // need to search the rows and update them
                ArrayList<String> ids = findAllIDs(updateCondition, updateConditionParameterMaps.get(i));
                executeAsBatch_noId_update(ids, updateSetParameterMap);
            } else {
                ArrayList<Object[]> ids = findAllUserDefinedIDs(updateCondition, updateConditionParameterMaps.get(i));
                executeAsBatch_nonPrime_update(ids, updateSetParameterMap);
            }
            i++;
        }
    }

    /**
     * This used to update a single row when the exact row key is provided
     *
     * @param updateSetParameterMap set parameters used to update the table
     * @param compiledCondition the compiledCondition against which records should be matched
     * @param updateConditionParameterMaps map of condition parameters
     */
    private void updateSingleRow(Map<String, Object> updateSetParameterMap, CompiledCondition compiledCondition,
                                 Map<String, Object> updateConditionParameterMaps) {
        ArrayList<String> keys = new ArrayList<>();
        String compiledQuery = ((CassandraCompiledCondition) compiledCondition).getCompiledQuery();
        for (Map.Entry<String, Object> parameter : updateSetParameterMap.entrySet()) {
            if (this.persistedKeyColumns.containsKey(parameter.getKey())) {
                keys.add(parameter.getKey());
            }
        }
        //Since cassandra cannot update a primary key column we need to remove the primary key values sent
        for (String key : keys) {
            updateSetParameterMap.remove(key);
        }
        ArrayList<Object> allValues = buildCQLStatement_Update(updateSetParameterMap);
        String finalUpdateQuery = updateQuery.replace(PLACEHOLDER_CONDITION, compiledQuery);
        Object[] argSet = constructArgSet(compiledCondition, updateConditionParameterMaps);
        allValues.addAll(new ArrayList<>(Arrays.asList(argSet)));
        LOG.info(finalUpdateQuery);
        PreparedStatement prepared = this.session.prepare(finalUpdateQuery);
        BoundStatement bound = prepared.bind(allValues.toArray());
        this.session.execute(bound);
    }

    /**
     * This used to construct the CQL query to a given set parameters
     *
     * @param updateParameterMap set parameters used to update the table
     */
    private ArrayList<Object> buildCQLStatement_Update(Map<String, Object> updateParameterMap) {
        String updateCql = this.configElements.getRecordUpdateQuery().replace(PLACEHOLDER_KEYSPACE, this.keyspace).
                replace(PLACEHOLDER_TABLE, this.tableName);
        ArrayList<Object> setValues = new ArrayList<>();
        StringBuilder updateParameters = new StringBuilder();
        buildUpdateParameterValues(updateParameterMap, setValues, updateParameters);
        this.updateQuery = updateCql.replace(PLACEHOLDER_COLUMNS_AND_VALUES, updateParameters.toString());
        LOG.info(this.updateQuery + "" + setValues.size());
        return setValues;
    }

    /**
     * This used to update a single row when the exact row key is provided
     *
     * @param updateParameterMap set parameters used to update the table
     * @param setValues a list of set values that is used to construct the update query
     * @param updateParameters the string that is to be constructed
     */
    private void buildUpdateParameterValues(Map<String, Object> updateParameterMap, ArrayList<Object> setValues,
                                            StringBuilder updateParameters) {
        int i = 1;
        int size = updateParameterMap.size();
        for (Map.Entry<String, Object> parameter : updateParameterMap.entrySet()) {
            updateParameters.append(parameter.getKey()).append(EQUALS).append(QUESTION_MARK);
            setValues.add(parameter.getValue());
            if (i != size) {
                updateParameters.append(SEPARATOR);
            }
            i++;
        }
    }

    /**
     * This is used to build the update set statement
     *
     * @param updateParameterMap set parameters used to update the table
     */
    private ArrayList<Object> buildUpdateSetStatement(Map<String, Object> updateParameterMap) {
        ArrayList<Object> setValues = buildCQLStatement_Update(updateParameterMap);
        StringBuilder condition = new StringBuilder();
        int size = this.persistedKeyColumns.size();
        int i = 1;
        for (Map.Entry<String, String> column : this.persistedKeyColumns.entrySet()) {
            condition.append(column.getKey()).append(EQUALS).append(QUESTION_MARK);
            if (i != size) {
                condition.append(WHITESPACE).append(CQL_AND).append(WHITESPACE);
            }
            i++;
        }
        this.updateQuery = this.updateQuery.replace(PLACEHOLDER_CONDITION, condition);
        LOG.info(this.updateQuery);
        return setValues;
    }

    @Override
    protected void updateOrAdd(CompiledCondition updateCondition, List<Map<String,
            Object>> updateConditionParameterMaps, Map<String, CompiledExpression> updateSetExpressions,
                               List<Map<String, Object>> updateSetParameterMaps, List<Object[]> addingRecords)
            throws ConnectionUnavailableException {
        int i = 0;
        for (Map<String, Object> updateSetParameterMap : updateSetParameterMaps) {
            if (containsAllPrimaryKeys(updateConditionParameterMaps.get(i)) && !(noKeyTable) &&
                    ((CassandraCompiledCondition) updateCondition).getReadOnlyCondition()) {
                updateOrAddSingleRow(updateSetParameterMap, updateCondition, updateConditionParameterMaps.get(i));
            } else if (this.noKeyTable) {
                // need to search the rows and update them
                ArrayList<String> ids = findAllIDs(updateCondition, updateConditionParameterMaps.get(i));
                if (ids.size() == 0) {
                    // need to insert
                    if (!this.persistedKeyColumns.containsKey("_id")) {
                        throw new CassandraTableException("No results found for the given values. In case of a query " +
                                "without primary keys, only update operation is possible. Since there are no matching" +
                                "this error occurs. If update or insert operation is needed whole key should be " +
                                "included");
                    } else {
                        //inserting into the no key table
                        updateOrAdd_NoKeyTable(updateSetParameterMap);
                    }

                } else {
                    // need to update
                    executeAsBatch_noId_update(ids, updateSetParameterMap);
                }
            } else {
                // updating key defined table
                ArrayList<Object[]> ids = findAllUserDefinedIDs(updateCondition, updateConditionParameterMaps.get(i));

                if (ids.size() == 0) {
                    throw new CassandraTableException("No results found for the given values. Only update " +
                            "functionality is capable without primary keys. If update or insert operation is needed" +
                            "whole key should be included");
                } else {
                    executeAsBatch_nonPrime_update(ids, updateSetParameterMap);
                }
                i++;
            }

        }
    }

    /**
     * This is used to updateOrAdd a row in table where primary key is not defined
     *
     * @param updateSetParameterMap set parameters used to update the table
     */
    private void updateOrAdd_NoKeyTable(Map<String, Object> updateSetParameterMap) {
        StringBuilder parameters = new StringBuilder();
        ArrayList<Object> setValues = new ArrayList<>();
        buildUpdateParameterValues(updateSetParameterMap, setValues, parameters);
        String condition = "\"_id\"" + EQUALS + QUESTION_MARK;
        String updateQuery = this.updateQuery.replace(PLACEHOLDER_COLUMNS_AND_VALUES, parameters.toString()).
                replace(PLACEHOLDER_CONDITION, condition);
        setValues.add(CassandraTableUtils.generatePrimaryKeyValue());

        PreparedStatement prepared = this.session.prepare(updateQuery);
        BoundStatement bound = prepared.bind(setValues.toArray());
        this.session.execute(bound);
        LOG.info(this.updateQuery + "" + setValues.size());
    }

    /**
     * This will call updateSingleRow (updating logic) used in update method
     *
     * @param updateSetParameterMap set parameters used to update the table
     * @param compiledCondition the compiledCondition against which records should be matched
     * @param updateConditionParameterMaps map of condition parameters
     */
    private void updateOrAddSingleRow(Map<String, Object> updateSetParameterMap, CompiledCondition compiledCondition,
                                      Map<String, Object> updateConditionParameterMaps) {
        updateSingleRow(updateSetParameterMap, compiledCondition, updateConditionParameterMaps);
    }

    /**
     * This will check whether a certain condition contains all the primary keys in the table
     * @return returns true if the condition condition contains all the primary keys
     */
    private boolean containsAllPrimaryKeys(Map<String, Object> paramList) {
        // null checking
        ArrayList<String> paramKeys = new ArrayList<>(paramList.keySet());
        if (this.persistedKeyColumns == null) {
            return false;
        } else if (paramKeys.size() != this.persistedKeyColumns.size()) {
            return false;
        }
        for (Map.Entry<String, String> persistedColumn : this.persistedKeyColumns.entrySet()) {
            if (!paramKeys.contains(persistedColumn.getKey())) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected CompiledCondition compileCondition(ExpressionBuilder expressionBuilder) {
        CassandraConditionVisitor visitor = new CassandraConditionVisitor();
        expressionBuilder.build(visitor);
        return new CassandraCompiledCondition(visitor.returnCondition(), visitor.getParameters(),
                visitor.getReadOnlyCondition());
    }

    @Override
    protected CompiledExpression compileSetAttribute(ExpressionBuilder expressionBuilder) {
        return compileCondition(expressionBuilder);
    }

    @Override
    protected void connect() throws ConnectionUnavailableException {
        //creating Cluster object
        String username, password;
        username = this.storeAnnotation.getElement(ANNOTATION_USER_NAME);
        password = this.storeAnnotation.getElement(ANNOTATION_PASSWORD);
        Cluster cluster = Cluster.builder().addContactPoint(host).withCredentials(username, password).build();
        this.session = cluster.connect(this.storeAnnotation.getElement(this.keyspace));
        checkTable();
    }

    /**
     * This will check whether the table is created if not will create the table
     */
    private void checkTable() {
        String checkStatement = this.configElements.getTableCheckQuery().replace(PLACEHOLDER_KEYSPACE,
                this.keyspace.toLowerCase(Locale.ENGLISH)).replace(PLACEHOLDER_TABLE,
                this.tableName.toLowerCase(Locale.ENGLISH));
        LOG.info(checkStatement);
        ResultSet result = this.session.execute(checkStatement);

        if (validateKP_Table()) {
            if (result.one() == null) {
                createTable();
            } else if (!validColumns_Table()) {
                throw new CassandraTableException("Problem with the table definition or key. " +
                        "Please re check the table schema and try again.");
            }
            //Otherwise table is already created.
        } else {
            throw new CassandraTableException("Invalid table name or Keyspace name. " +
                    "Please refer the cassandra documentation for naming valid table and keyspace");
        }
    }

    /**
     * This will check whether the table is created if not will create the table
     */
    private void createTable() {
        StringBuilder primaryKeyStatement = new StringBuilder();
        primaryKeyStatement.append(CQL_PRIMARY_KEY_DEF).append(OPEN_PARENTHESIS);
        int i = 0;
        for (Attribute primaryKey : this.primaryKeys) {
            primaryKeyStatement.append(primaryKey.getName());
            if (i != this.primaryKeys.size() - 1) {
                primaryKeyStatement.append(SEPARATOR);
            } else {
                primaryKeyStatement.append(")");
            }
            i++;
        }
        StringBuilder attributeStatement = new StringBuilder();
        String type;
        for (Attribute attribute : this.schema) {
            attributeStatement.append(attribute.getName());
            attributeStatement.append(WHITESPACE);
            type = CassandraTableUtils.dataConversionToCassandra(attribute.getType());
            attributeStatement.append(type);
            attributeStatement.append(SEPARATOR);
        }
        //when primary key is not given
        if (this.noKeys) {
            attributeStatement.append("\"_id\" text").append(WHITESPACE).append(SEPARATOR);
            this.noKeyTable = true;
        }

        String createStatement = this.configElements.getTableCreateQuery().replace(PLACEHOLDER_KEYSPACE, this.keyspace).
                replace(PLACEHOLDER_TABLE, this.tableName).replace(PLACEHOLDER_COLUMNS, attributeStatement).
                replace(PLACEHOLDER_PRIMARY_KEYS, primaryKeyStatement);
        LOG.info(createStatement);
        session.execute(createStatement);

        if (this.indexAnnotation != null) {
            LOG.info(this.indexAnnotation);
            initIndexQuery();
        }
        findPersistedKeys();
    }

    /**
     * User defined keys that are actually defined in the keyspace
     */
    private void findPersistedKeys() {
        String checkStatement = this.configElements.getTableValidityQuery().replace(PLACEHOLDER_KEYSPACE,
                this.keyspace.toLowerCase(Locale.ENGLISH)).
                replace(PLACEHOLDER_TABLE, this.tableName.toLowerCase(Locale.ENGLISH));
        ResultSet result = this.session.execute(checkStatement);
        ArrayList<TableMeta> tableColumns = new ArrayList<>();

        for (Row row : result) {
            TableMeta tableMeta = new TableMeta(row.getString("column_name"),
                    row.getString("kind"), row.getString("type"));
            tableColumns.add(tableMeta);
        }

        this.persistedKeyColumns = new HashMap<>();
        for (TableMeta column : tableColumns) {
            if (column.getKeyType().equals("partition_key") || column.getKeyType().equals("clustering")) {
                this.persistedKeyColumns.put(column.getColumnName(), column.getDataType());
            }
        }
    }

    /**
     * This will check whether the table and keyspace is valid before creating the table
     */
    private boolean validateKP_Table() {
        String pattern = "^[A-Za-z0-9_]*$";
        Pattern r = Pattern.compile(pattern);
        Matcher kp = r.matcher(this.keyspace);
        Matcher table = r.matcher(this.tableName);
        return (kp.find() && table.find());
    }


    /**
     * This will check whether defined column already exists in the table and the defined primary keys
     * are already as the previously defined ones
     */
    private boolean validColumns_Table() {
        String checkStatement = this.configElements.getTableValidityQuery().replace(PLACEHOLDER_KEYSPACE,
                this.keyspace.toLowerCase(Locale.ENGLISH)).
                replace(PLACEHOLDER_TABLE, this.tableName.toLowerCase(Locale.ENGLISH));
        LOG.info(checkStatement);
        ResultSet result = this.session.execute(checkStatement);
        HashMap<String, String> tableDet = new HashMap<>();
        ArrayList<TableMeta> tableColumns = new ArrayList<>();
        for (Row row : result) {
            tableDet.put(row.getString("column_name"), row.getString("kind"));
            TableMeta tableMeta = new TableMeta(row.getString("column_name"),
                    row.getString("kind"), row.getString("type"));
            tableColumns.add(tableMeta);
        }

        this.persistedKeyColumns = new HashMap<>();
        HashMap<String, String> persistedColumns = new HashMap<>();
        for (TableMeta column : tableColumns) {
            if (column.getKeyType().equals("partition_key") || column.getKeyType().equals("clustering")) {
                this.persistedKeyColumns.put(column.getColumnName(), column.getDataType());
            }
            persistedColumns.put(column.getColumnName(), column.getDataType());
        }

        boolean validKeys = false, validColumns, validDataTypes;
        // To Check whether the column names match with the persisted column names
        for (Attribute attribute : this.schema) {
            validColumns = tableDet.containsKey(attribute.getName().toLowerCase(Locale.ENGLISH));
            if (!validColumns) {
                return false;
            }
        }

        // To Check whether the column data types match with the persisted column data types names
        for (Attribute attribute : this.schema) {
            String persistedColumnType = persistedColumns.get(attribute.getName().toLowerCase(Locale.ENGLISH));
            String inComingDataType = CassandraTableUtils.dataConversionToCassandra(attribute.getType());
            validDataTypes = persistedColumnType.equalsIgnoreCase(inComingDataType);
            if (!validDataTypes) {
                return false;
            }
        }

        if (this.noKeys) {
            validKeys = tableDet.containsKey("_id");
            this.noKeyTable = validKeys;
        } else {
            for (Attribute attribute : this.primaryKeys) {
                String pk = attribute.getName().toLowerCase(Locale.ENGLISH);
                validKeys = (tableDet.containsKey(pk) &&
                        (tableDet.get(pk).equals("partition_key") || tableDet.get(pk).equals("clustering")));
                if (!validKeys) {
                    return false;
                }
            }
        }
        return validKeys;
    }

    /**
     * This check the indexAnnotation and create indexes on the given columns
     */
    private void initIndexQuery() {
        String[] indexes = this.indexAnnotation.getElements().get(0).getValue().split(SEPARATOR);
        for (String index : indexes) {
            for (Attribute attribute : this.schema) {
                if (attribute.getName().trim().equals(index)) {
                    String indexQuery = this.configElements.getIndexQuery().
                            replace(PLACEHOLDER_KEYSPACE, this.keyspace).replace(PLACEHOLDER_TABLE, this.tableName).
                            replace(PLACEHOLDER_INDEX, index);
                    this.session.execute(indexQuery);
                }
            }
        }
    }

    /**
     * This will read the cassandra-table-config.xml file which contains the syntax for Cassandra.
     */
    private void readConfigFile() throws JAXBException {
        File file = new File("src/test/resources/cassandra-table-config.xml");
        JAXBContext jaxbContext = JAXBContext.newInstance(ConfigElements.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        this.configElements = (ConfigElements) jaxbUnmarshaller.unmarshal(file);
    }

    @Override
    protected void disconnect() {
        this.session.close();
    }

    @Override
    protected void destroy() {
    }
}
