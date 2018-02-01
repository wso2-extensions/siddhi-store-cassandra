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
package org.wso2.extension.siddhi.store.cassandra.iterator;

import com.datastax.driver.core.Row;
import org.wso2.extension.siddhi.store.cassandra.exception.CassandraTableException;
import org.wso2.siddhi.core.table.record.RecordIterator;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * A class representing a RecordIterator which is responsible for processing Cassandra Event Table find() operations in
 * a streaming fashion.
 */
public class CassandraIterator implements RecordIterator<Object[]> {

    private Iterator<Row> cassandraIterator;
    private List<Attribute> attributes;

    public CassandraIterator(Iterator<Row> cassandraIterator, List<Attribute> attributes) {

        this.cassandraIterator = cassandraIterator;
        this.attributes = attributes;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     * <p>
     * <p> As noted in {@link AutoCloseable#close()}, cases where the
     * close may fail require careful attention. It is strongly advised
     * to relinquish the underlying resources and to internally
     * <em>mark</em> the {@code Closeable} as closed, prior to throwing
     * the {@code IOException}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        return this.cassandraIterator.hasNext();
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     */
    @Override
    public Object[] next() {
        Row row = cassandraIterator.next();
        return extractRecord(row);
    }

    /**
     * Method which is used for extracting record values (in the form of an Object array) from an CQL {@link Row},
     * according to the table's field type order.
     *
     * @param row the {@link Row} from which the values should be retrieved.
     * @return an array of extracted values, all cast to {@link Object} type for portability.
     *             to the table definition
     */
    private Object[] extractRecord(Row row) {
        List<Object> result = new ArrayList<>();
        this.attributes.forEach(attribute -> {
            switch (attribute.getType()) {
                case BOOL:
                    result.add(row.getBool(attribute.getName()));
                    break;
                case DOUBLE:
                    result.add(row.getDouble(attribute.getName()));
                    break;
                case FLOAT:
                    result.add(row.getFloat(attribute.getName()));
                    break;
                case INT:
                    try {
                        result.add(objectDataReadResolver(row, attribute.getName()));
                    } catch (IOException | ClassNotFoundException ex) {
                        throw new CassandraTableException("Something went wrong when retrieving an object", ex);
                    }
                    break;
                case LONG:
                    result.add(row.getLong(attribute.getName()));
                    break;
                case OBJECT:
                    result.add(row.getObject(attribute.getName()));
                    break;
                case STRING:
                    result.add(row.getString(attribute.getName()));
                    break;
                default:
                    //Operation not supported
            }
        });
        return result.toArray();
    }

    private Object objectDataReadResolver(Row row, String attributeName) throws IOException, ClassNotFoundException {
        ByteBuffer data = row.getBytes(attributeName);
        ByteArrayInputStream bis = new ByteArrayInputStream(data.array());
        ObjectInput in = new ObjectInputStream(bis);
        Object object = in.readObject();
        in.close();
        return object;
    }
}
