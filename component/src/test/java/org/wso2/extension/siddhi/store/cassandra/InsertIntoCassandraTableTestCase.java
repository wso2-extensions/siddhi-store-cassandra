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
import org.wso2.extension.siddhi.store.cassandra.utils.TestOb;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.HOST;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.KEY_SPACE;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.USER_NAME;

public class InsertIntoCassandraTableTestCase {


    private static final Logger log = Logger.getLogger(InsertIntoCassandraTableTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeClass
    public static void startTest() {
        log.info("== Cassandra Table INSERTION tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Cassandra Table INSERTION tests completed ==");
    }

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        CassandraTableTestUtils.initializeTable();
    }

    @Test(testName = "cassandratableinsertiontest1", description = "Testing table creation.")
    public void cassandrainsertiontest1() throws InterruptedException {
        //Configure siddhi to insert events data to the HBase table only from specific fields of the stream.
        log.info("casandrainsertiontest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", table.name=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + HOST + "\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 700L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT", 57.6F, 100L});
                                break;
                            default:
                                Assert.assertEquals(3, inEventCount);
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

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 700L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 700L});
        checkStockStream.send(new Object[]{"MSFT", 57.6F, 100L});

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "cassandratableinsertiontest2", description = "Testing table creation.")
    public void cassandratableinsertiontest2() throws InterruptedException {
        //Testing table creation and insetion with an object
        log.info("casandrainsertiontest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long, ob object); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", table.name=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + HOST + "\")" +
                "@PrimaryKey(\"symbol, price\")" +
                "define table StockTable (symbol string, price float,volume long, ob object); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume, ob\n" +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO3", 55.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 700L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT", 57.3F, 400L});
                                break;
                            default:
                                Assert.assertEquals(3, inEventCount);
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
        stockStream.send(new Object[]{"WSO3", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 700L});
        stockStream.send(new Object[]{"MSFT", 57.3F, 400L});

        checkStockStream.send(new Object[]{"WSO3", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 700L});
        checkStockStream.send(new Object[]{"MSFT", 57.3F, 400L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "cassandratableinsertiontest3", description = "Testing table creation.")
    public void cassandratableinsertiontest3() throws InterruptedException {
        //Testing table creation with a compound primary key field
        log.info("casandrainsertiontest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long, ob object); " +
                "define stream CheckStockStream (symbol string, price float, volume long, ob object); " +
                "@Store(type=\"cassandra\", table.name=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + HOST + "\")" +
                "@PrimaryKey(\"symbol, price\")" +
                "define table StockTable (symbol string, price float,volume long, ob object); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, price, volume, ob\n" +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

        log.info(streams + query);
        TestOb testOb = new TestOb("1", "Tharindu");
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 100L, testOb});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L , testOb});
                                break;
                            case 3:
                                Assert.assertEquals (event.getData(),
                                        new Object[]{"MSFT", 57.6F, 100L , testOb});
                                break;
                            default:
                                Assert.assertEquals(3, inEventCount);
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

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L, testOb});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L , testOb});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L , testOb});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L, testOb});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L , testOb});
        checkStockStream.send(new Object[]{"MSFT", 57.6F, 100L , testOb});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }


}
