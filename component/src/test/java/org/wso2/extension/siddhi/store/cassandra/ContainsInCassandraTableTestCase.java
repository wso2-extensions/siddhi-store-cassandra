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

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.KEY_SPACE;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.USER_NAME;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.getHostIp;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.getPort;


public class ContainsInCassandraTableTestCase {
    private static final Logger log = Logger.getLogger(ContainsInCassandraTableTestCase.class);
    private int removeEventCount;
    private boolean eventArrived;
    private AtomicInteger inEventCount;

    @BeforeMethod
    public void init() {
        removeEventCount = 0;
        eventArrived = false;
        inEventCount = new AtomicInteger(0);
        CassandraTableTestUtils.initializeTable();
    }

    @BeforeClass
    public static void startTest() {
        log.info("== Contains in Table tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Contains in Table tests completed ==");
    }

    @Test
    public void containsCassandraTableTest1() throws InterruptedException {
        log.info("containsCassandraTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
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
                "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 100L});
                                break;
                            default:
                                Assert.assertEquals(2, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        SiddhiTestHelper.waitForEvents(200, 2, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 2, "Number of success events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        long totalRowsInTable = CassandraTableTestUtils.getRowsInTable();
        Assert.assertEquals(totalRowsInTable, 2, "Update operation failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "containsCassandraTableTest1")
    public void containsCassandraTableTest2() throws InterruptedException {
        log.info("containsCassandraTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
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
                "insert into StockTable; " +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 100L},
                                        "Event should be {IBM, 100}");
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 100L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 100L});
                                break;
                            default:
                                Assert.assertEquals(inEventCount, 2);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"MSFT", 22.9f, 200L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        SiddhiTestHelper.waitForEvents(200, 2, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 3, "Number of success events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        long totalRowsInTable = CassandraTableTestUtils.getRowsInTable();
        Assert.assertEquals(totalRowsInTable, 3, "Update operation failed");
        siddhiAppRuntime.shutdown();
    }


    @Test(dependsOnMethods = "containsCassandraTableTest2")
    public void containsCassandraTableTest3() throws InterruptedException {
        log.info("containsCassandraTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
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
                "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 100L});
                                break;
                            default:
                                Assert.assertEquals(2, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        SiddhiTestHelper.waitForEvents(200, 2, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 2, "Number of success events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        long totalRowsInTable = CassandraTableTestUtils.getRowsInTable();
        Assert.assertEquals(totalRowsInTable, 2, "Update operation failed");
        siddhiAppRuntime.shutdown();
    }
}
