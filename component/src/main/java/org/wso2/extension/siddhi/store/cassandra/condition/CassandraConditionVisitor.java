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

import org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants;
import org.wso2.extension.siddhi.store.cassandra.util.CassandraTableUtils;
import org.wso2.extension.siddhi.store.cassandra.util.Constant;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.table.record.BaseExpressionVisitor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.condition.Compare;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.CLOSE_SQUARE_BRACKET;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.OPEN_SQUARE_BRACKET;
import static org.wso2.extension.siddhi.store.cassandra.util.CassandraEventTableConstants.QUESTION_MARK;

/**
 * Class which is used by the Siddhi runtime for instructions on converting the SiddhiQL condition to the condition
 * format understood by the underlying Cassandra data store.
 */
public class CassandraConditionVisitor extends BaseExpressionVisitor {

    private StringBuilder condition;
    private String finalCompiledCondition;

    private Map<String, Object> placeholders;
    private SortedMap<Integer, Object> parameters;
    private int streamVarCount;
    private int constantCount;
    private boolean streamVarVisited;
    private boolean constantVisited;
    private boolean storeVarVisited;
    private Operand.StreamVariable streamVariable;
    private Constant constant;
    private String cqlOperatorString;
    private boolean readOnlyCondition;

    public CassandraConditionVisitor() {
        this.condition = new StringBuilder();
        this.streamVarCount = 0;
        this.constantCount = 0;
        this.placeholders = new HashMap<>();
        this.parameters = new TreeMap<>();
        this.readOnlyCondition = true;
    }

    /**
     * This returns the compile condition needed to convert siddhi query into cassandra query.
     */
    public String returnCondition() {
        this.parametrizeCondition();
        return this.finalCompiledCondition.trim();
    }

    public boolean getReadOnlyCondition() {
        return readOnlyCondition;
    }

    /**
     * This method returns a sorted map which is needed to map with the siddhi parameters.
     * @return Sorted map is returned with the relevent parameters.
     */
    public SortedMap<Integer, Object> getParameters() {
        return this.parameters;
    }

    @Override
    public void beginVisitAnd() {
        //Not applicable
    }

    @Override
    public void endVisitAnd() {
        //Not applicable
    }

    @Override
    public void beginVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void endVisitAndLeftOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitAndRightOperand() {
        condition.append(CassandraEventTableConstants.CQL_AND).append(CassandraEventTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitAndRightOperand() {
        //Not applicable
    }

    @Override
    public void beginVisitOr() {
        throw new OperationNotSupportedException("OR operator is not supported in cassandra");
    }

    @Override
    public void endVisitOr() {
        throw new OperationNotSupportedException("OR operator is not supported in cassandra");
    }

    @Override
    public void beginVisitOrLeftOperand() {
        throw new OperationNotSupportedException("OR operator is not supported in cassandra");
    }

    @Override
    public void endVisitOrLeftOperand() {
        throw new OperationNotSupportedException("OR operator is not supported in cassandra");
    }

    @Override
    public void beginVisitOrRightOperand() {
        throw new OperationNotSupportedException("OR operator is not supported in cassandra");
    }

    @Override
    public void endVisitOrRightOperand() {
        throw new OperationNotSupportedException("OR operator is not supported in cassandra");
    }

    @Override
    public void beginVisitNot() {
        throw new OperationNotSupportedException("NOT operator is not supported in cassandra");
    }

    @Override
    public void endVisitNot() {
        throw new OperationNotSupportedException("NOT_EQUAL operator is not supported in cassandra");
    }

    @Override
    public void beginVisitCompare(Compare.Operator operator) {
        streamVarVisited = false;
        constantVisited = false;
        storeVarVisited = false;
        condition.append(CassandraEventTableConstants.OPEN_PARENTHESIS);
    }

    @Override
    public void endVisitCompare(Compare.Operator operator) {
        condition.append(CassandraEventTableConstants.CLOSE_PARENTHESIS);
        condition.append(CassandraEventTableConstants.WHITESPACE);
    }

    @Override
    public void beginVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void endVisitCompareLeftOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitCompareRightOperand(Compare.Operator operator) {
        if (streamVarVisited || constantVisited) {
            this.cqlOperatorString = getCorrespondingCqlOperatorString(operator);
        } else {
            condition.append(getCorrespondingCqlOperatorString(operator));
            condition.append(CassandraEventTableConstants.WHITESPACE);
        }
    }

    /**
     * This method returns the converted operator corresponding to cassandra.
     * @param operator operator that need to perform the function.
     */
    private String getCorrespondingCqlOperatorString(Compare.Operator operator) {
        String cqlOPStr = "";
        switch (operator) {
            case EQUAL:
                cqlOPStr = CassandraEventTableConstants.CQL_COMPARE_EQUAL;
                break;
            case GREATER_THAN:
                cqlOPStr = CassandraEventTableConstants.CQL_GREATER_THAN;
                readOnlyCondition = false;
                break;
            case GREATER_THAN_EQUAL:
                cqlOPStr = CassandraEventTableConstants.CQL_GREATER_THAN_EQUAL;
                readOnlyCondition = false;
                break;
            case LESS_THAN:
                cqlOPStr = CassandraEventTableConstants.CQL_LESS_THAN;
                readOnlyCondition = false;
                break;
            case LESS_THAN_EQUAL:
                cqlOPStr = CassandraEventTableConstants.CQL_LESS_THAN_EQUAL;
                readOnlyCondition = false;
                break;
            case NOT_EQUAL:
                throw new OperationNotSupportedException("NOT_EQUAL operator is not supported in cassandra");
        }
        return cqlOPStr;
    }

    @Override
    public void endVisitCompareRightOperand(Compare.Operator operator) {
        //Not applicable
    }

    @Override
    public void beginVisitIsNull(String streamId) {
        //Not applicable
    }

    @Override
    public void endVisitIsNull(String streamId) {
        //Not applicable
    }

    @Override
    public void beginVisitIn(String storeId) {
        condition.append(CassandraEventTableConstants.CQL_IN).append(CassandraEventTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitIn(String storeId) {
        //Not applicable
    }

    @Override
    public void beginVisitConstant(Object value, Attribute.Type type) {
        this.constant = new Constant(value, type);
        if (storeVarVisited || constantVisited) {
            processConstant();
        }
        constantVisited = true;
    }

    @Override
    public void endVisitConstant(Object value, Attribute.Type type) {
        //Not applicable
    }

    @Override
    public void beginVisitMath(MathOperator mathOperator) {
        throw new OperationNotSupportedException("Math operators at the beginning of an operation is not supported " +
                "in cassandra");
    }

    @Override
    public void endVisitMath(MathOperator mathOperator) {
        throw new OperationNotSupportedException("Math operators at the beginning of an operation is not supported " +
                "in cassandra");
    }

    @Override
    public void beginVisitMathLeftOperand(MathOperator mathOperator) {
        throw new OperationNotSupportedException("Math operators at the beginning of an operation is not supported " +
                "in cassandra");
    }

    @Override
    public void endVisitMathLeftOperand(MathOperator mathOperator) {
        throw new OperationNotSupportedException("Math operators at the beginning of an operation is not supported " +
                "in cassandra");
    }

    @Override
    public void beginVisitMathRightOperand(MathOperator mathOperator) {
        throw new OperationNotSupportedException("Math operators at the beginning of an operation is not supported " +
                "in cassandra");
    }

    @Override
    public void endVisitMathRightOperand(MathOperator mathOperator) {
        throw new OperationNotSupportedException("Math operators at the beginning of an operation is not supported " +
                "in cassandra");
    }

    @Override
    public void beginVisitAttributeFunction(String namespace, String functionName) {
        if (CassandraTableUtils.isEmpty(namespace)) {
            condition.append(functionName).append(CassandraEventTableConstants.OPEN_PARENTHESIS);
        } else {
            throw new OperationNotSupportedException("The Cassandra Event table does not support function " +
                    "namespaces, but namespace '" + namespace + "' was specified. Please use functions supported by " +
                    "the defined Cassandra data store.");
        }
    }

    @Override
    public void endVisitAttributeFunction(String namespace, String functionName) {
        if (CassandraTableUtils.isEmpty(namespace)) {
            condition.append(CassandraEventTableConstants.OPEN_PARENTHESIS).
                    append(CassandraEventTableConstants.WHITESPACE);
        } else {
            throw new OperationNotSupportedException("The Cassandra Event table does not support function " +
                    "namespaces, but namespace '" + namespace + "' was specified. Please use functions supported by " +
                    "the defined Cassandra data store.");
        }
    }

    @Override
    public void beginVisitParameterAttributeFunction(int index) {
        throw new OperationNotSupportedException("ParameterAttributeFunction are not supported in cassandra");
    }

    @Override
    public void endVisitParameterAttributeFunction(int index) {
        throw new OperationNotSupportedException("ParameterAttributeFunction are not supported in cassandra");
    }

    @Override
    public void beginVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        this.streamVariable = new Operand.StreamVariable(id, type);
        if (storeVarVisited || constantVisited) {
            processStreamVariable();
        }
        streamVarVisited = true;
    }

    /**
     * This method is used to process stream variable which could be used whenever possible.
     */
    private void processStreamVariable() {
        String name = this.generateStreamVarName();
        if (this.streamVariable != null) {
            this.placeholders.put(name, new Attribute(streamVariable.getName(), streamVariable.getType()));
        }
        condition.append("[").append(name).append("]").append(CassandraEventTableConstants.WHITESPACE);
    }

    /**
     * This method is used to process constant which could be used whenever possible.
     */
    private void processConstant() {
        String name = this.generateConstantName();
        this.placeholders.put(name, this.constant);
        condition.append("[").append(name).append("]").append(CassandraEventTableConstants.WHITESPACE);
    }

    @Override
    public void endVisitStreamVariable(String id, String streamId, String attributeName, Attribute.Type type) {
        //Not applicable
    }

    @Override
    public void beginVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        condition.append(attributeName).
                append(CassandraEventTableConstants.WHITESPACE);
        if (streamVarVisited) {
            condition.append(cqlOperatorString).append(CassandraEventTableConstants.WHITESPACE);
            processStreamVariable();
        } else if (constantVisited) {
            condition.append(cqlOperatorString).append(CassandraEventTableConstants.WHITESPACE);
            processConstant();
        }
        storeVarVisited = true;
    }

    @Override
    public void endVisitStoreVariable(String storeId, String attributeName, Attribute.Type type) {
        //Not applicable
    }

    /**
     * Util method for walking through the generated condition string and isolating the parameters which will be filled
     * in later as part of building the SQL statement. This method will:
     * (a) eliminate all temporary placeholders and put "?" in their places.
     * (b) build and maintain a sorted map of ordinals and the coresponding parameters which will fit into the above
     * places in the PreparedStatement.
     */
    private void parametrizeCondition() {
        String query = this.condition.toString();
        String[] tokens = query.split("\\[");
        int ordinal = 1;
        for (String token : tokens) {
            if (token.contains(CLOSE_SQUARE_BRACKET)) {
                String candidate = token.substring(0, token.indexOf(CLOSE_SQUARE_BRACKET));
                if (this.placeholders.containsKey(candidate)) {
                    this.parameters.put(ordinal, this.placeholders.get(candidate));

                    ordinal++;
                }
            }
        }
        for (String placeholder : this.placeholders.keySet()) {
            query = query.replace(OPEN_SQUARE_BRACKET + placeholder + CLOSE_SQUARE_BRACKET, QUESTION_MARK);
        }
        this.finalCompiledCondition = query;
    }

    /**
     * Method for generating a temporary placeholder for stream variables.
     *
     * @return a placeholder string of known format.
     */
    private String generateStreamVarName() {
        String name = "strVar" + this.streamVarCount;
        this.streamVarCount++;
        return name;
    }

    /**
     * Method for generating a temporary placeholder for constants.
     *
     * @return a placeholder string of known format.
     */
    private String generateConstantName() {
        String name = "const" + this.constantCount;
        this.constantCount++;
        return name;
    }
}
