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

import io.siddhi.query.api.definition.Attribute;

/**
 * This abstract class acts as a template for the 3 different kinds of variables that can be encountered at
 * condition evaluation. All kinds will contain a variable type.
 */
public abstract class Operand {
    protected Attribute.Type type;

    /**
     * This method is to return type of the attribute.
     * @return the type of the variable
     */
    public Attribute.Type getType() {
        return type;
    }

    /**
     * Class denoting a stream variable, which will contain a type and a name.
     */
    public static class StreamVariable extends Operand {
        private String name;

        StreamVariable(String name, Attribute.Type type) {
            this.name = name;
            this.type = type;
        }

        /**
         * This method is used to return the name of the stream variable.
         * @return name of the stream variable
         */
        public String getName() {
            return name;
        }
    }
}
