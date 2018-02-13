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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.KEY_SPACE;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.USER_NAME;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.getHostIp;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.getPort;

public class DeleteFromCassandraTableTestCase {
    private static final Log log = LogFactory.getLog(DeleteFromCassandraTableTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== Cassandra Table DELETE tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Cassandra Table DELETE tests completed ==");
    }

    @BeforeMethod
    public void init() {
        CassandraTableTestUtils.initializeTable();
    }

    @Test()
    public void deleteFromCassandraTableTest1() throws InterruptedException {
        // Testing simple deletion with primary keys.
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM"});
        deleteStockStream.send(new Object[]{"WSO2"});
        SiddhiTestHelper.waitForEvents(200, 5, new AtomicInteger(5), 10000);

        long totalRowsInTable = CassandraTableTestUtils.getRowsInTable();
        Assert.assertEquals(totalRowsInTable, 0, "Deletion failed");

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "deleteFromCassandraTableTest1")
    public void deleteFromCassandraTableTest2() throws InterruptedException {
        // Testing simple deletion with primary keys, operands in different order.
        log.info("deleteFromCassandraTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
               // "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        SiddhiTestHelper.waitForEvents(200, 5, new AtomicInteger(5), 10000);

        long totalRowsInTable = CassandraTableTestUtils.getRowsInTable();
        Assert.assertEquals(totalRowsInTable, 0, "Deletion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "deleteFromCassandraTableTest2")
    public void deleteFromCassandraTableTest3() throws InterruptedException {
        // Testing simple deletion with primary keys with one operand as a constant.
        log.info("deleteFromCassandraTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == 'IBM'  ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        SiddhiTestHelper.waitForEvents(200, 4, new AtomicInteger(4), 10000);

        long totalRowsInTable = CassandraTableTestUtils.getRowsInTable();
        Assert.assertEquals(totalRowsInTable, 1, "Deletion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "deleteFromCassandraTableTest3")
    public void deleteFromCassandraTableTest4() throws InterruptedException {
        // Testing simple deletion with primary keys with one operand as a constant, with the operand orders reversed.
        log.info("deleteFromCassandraTableTest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on 'IBM' == StockTable.symbol  ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        SiddhiTestHelper.waitForEvents(200, 4, new AtomicInteger(4), 10000);

        long totalRowsInTable = CassandraTableTestUtils.getRowsInTable();
        Assert.assertEquals(totalRowsInTable, 1, "Deletion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "deleteFromCassandraTableTest4")
    public void deleteFromCassandraTableTest5() throws InterruptedException {
        // Testing simple deletion with conditions. Expected to throw an exception since it is not supported.
        log.info("deleteFromCassandraTableTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol==symbol and StockTable.price > price and  StockTable.volume == volume  ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"IBM", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        SiddhiTestHelper.waitForEvents(200, 4, new AtomicInteger(4), 10000);

        long totalRowsInTable = CassandraTableTestUtils.getRowsInTable();
        Assert.assertEquals(totalRowsInTable, 1, "Deletion failed");

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "deleteFromCassandraTableTest5", dependsOnMethods = "deleteFromCassandraTableTest5")
    public void deleteFromCassandraTableTest6() throws InterruptedException {
        // Testing simple deletion with primary keys with stream variable name aliases.
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol_q string, price float, volume long); " +
                "define stream DeleteStockStream (symbol_q string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "select symbol_q as symbol, price, volume " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol_q ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        SiddhiTestHelper.waitForEvents(200, 5, new AtomicInteger(5), 10000);

        long totalRowsInTable = CassandraTableTestUtils.getRowsInTable();
        Assert.assertEquals(totalRowsInTable, 0, "Deletion failed");

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "deleteFromCassandraTableTest6")
    public void deleteFromCassandraTableTest7() throws InterruptedException {
        // Testing simple deletion without primary keys. Expected to throw an exception since it is not supported.
        log.info("deleteFromCassandraTableTest7");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, volume long); " +
                "define stream DeleteStockStream (symbol string, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "define table StockTable (symbol string, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 100L});
        stockStream.send(new Object[]{"IBM", 100L});
        stockStream.send(new Object[]{"WSO2", 100L});
        deleteStockStream.send(new Object[]{"WSO2", 100L});
        SiddhiTestHelper.waitForEvents(200, 4, new AtomicInteger(4), 10000);

        long totalRowsInTable = CassandraTableTestUtils.getRowsInTable();
        Assert.assertEquals(totalRowsInTable, 1, "Deletion failed");

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "deleteFromCassandraTableTest7")
    public void deleteFromCassandraTableTest8() throws InterruptedException {
        // Testing simple deletion without all of the primary keys. Expected to throw an exception since
        // it is not supported.
        log.info("deleteFromCassandraTableTest8");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "@PrimaryKey(\"symbol, price\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = CassandraTableTestUtils.getRowsInTable();
        Assert.assertEquals(totalRowsInTable, 2, "Deletion failed");

        siddhiAppRuntime.shutdown();
    }
}
