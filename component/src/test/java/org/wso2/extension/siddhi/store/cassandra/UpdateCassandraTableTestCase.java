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

public class UpdateCassandraTableTestCase {
    private static final Logger log = Logger.getLogger(UpdateCassandraTableTestCase.class);
    private AtomicInteger inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        inEventCount = new AtomicInteger(0);
        removeEventCount = 0;
        eventArrived = false;
        CassandraTableTestUtils.initializeTable();
    }

    @BeforeClass
    public static void startTest() {
        log.info("== Cassandra Table UPDATE tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Cassandra Table UPDATE tests completed ==");
    }

    @Test
    public void updateFromTableTest1() throws InterruptedException {
        log.info("updateFromTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2') " +
                "from UpdateStockStream\n" +
                "select symbol, price, volume\n" +
                "update StockTable\n" +
                "on (StockTable.symbol == symbol);" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 50.9F, 100L});
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});

        updateStockStream.send(new Object[]{"IBM", 50.9F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 50.9F, 100L});

        SiddhiTestHelper.waitForEvents(200, 2, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest1")
    public void updateFromTableTest2() throws InterruptedException {
        //Check for update event data in HBase table when multiple key conditions are true.
        log.info("updateFromTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price double, volume long); " +
                "define stream UpdateStockStream (symbol string, price double, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "@PrimaryKey(\"symbol, volume\")" +
                "define table StockTable (symbol string, price double, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update StockTable " +
                "   on (StockTable.symbol == symbol and StockTable.volume == volume) ;" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6, 50L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6, 100L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 100.1, 100L});
                                break;
                            default:
                                Assert.assertEquals(inEventCount, 3);
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6, 50L});
        stockStream.send(new Object[]{"IBM", 75.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 100L});
        updateStockStream.send(new Object[]{"WSO2", 100.1, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.6, 50L});
        checkStockStream.send(new Object[]{"IBM", 75.6, 100L});
        checkStockStream.send(new Object[]{"WSO2", 100.1, 100L});
        SiddhiTestHelper.waitForEvents(200, 3, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest2")
    public void updateFromTableTest3() throws InterruptedException {
        // Check update operations with keys other than the defined primary keys.
        log.info("updateFromTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
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
                "from UpdateStockStream " +
                "update StockTable " +
                "   on StockTable.volume == volume ;" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 57.6f, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 57.6f, 100L});
                                break;
                            default:
                                Assert.assertEquals(inEventCount, 3);
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});

        checkStockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        checkStockStream.send(new Object[]{"IBM", 57.6f, 100L});
        SiddhiTestHelper.waitForEvents(200, 2, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest3")
    public void updateFromTableTest4() throws InterruptedException {
        // Check update operations with not all primary keys in EQUALS form.
        log.info("updateFromTableTest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "@PrimaryKey(\"symbol, volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select symbol, price, volume " +
                "update StockTable " +
                "   on (StockTable.symbol == symbol and StockTable.volume > volume) ;" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 50L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 175L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 85.6F, 200L});
                                break;
                            default:
                                Assert.assertEquals(inEventCount, 3);
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 50L});
        stockStream.send(new Object[]{"IBM", 75.6F, 175L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 200L});

        updateStockStream.send(new Object[]{"WSO2", 85.6F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 50L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 175L});
        checkStockStream.send(new Object[]{"WSO2", 85.6F, 200L});
        SiddhiTestHelper.waitForEvents(200, 3, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest4")
    public void updateFromTableTest5() throws InterruptedException {
        // Check update operations with primary keys not in EQUALS form.
        log.info("updateFromTableTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
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
                "from UpdateStockStream " +
                "select symbol, price, volume " +
                "update StockTable " +
                "   on (StockTable.volume > volume) ;" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 85.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 85.6F, 100L});
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 150L});
        stockStream.send(new Object[]{"IBM", 75.6F, 175L});
        updateStockStream.send(new Object[]{"WSO2", 85.6F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 85.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 85.6F, 100L});
        SiddhiTestHelper.waitForEvents(200, 2, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");

        siddhiAppRuntime.shutdown();
    }

    //Without primary keys
    @Test(dependsOnMethods = "updateFromTableTest5")
    public void updateFromTableTest6() throws InterruptedException {
        //Check for update event data in HBase table when a primary key condition is true.
        log.info("updateFromTableTest6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2') " +
                "from UpdateStockStream\n" +
                "select symbol, price, volume\n" +
                "update StockTable\n" +
                "on (StockTable.symbol == symbol);" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 160L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 57.6F, 100L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 50.9F, 110L});
                                break;
                            default:
                                Assert.assertEquals(inEventCount, 3);
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 160L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 140L});
        updateStockStream.send(new Object[]{"IBM", 50.9F, 110L});
        SiddhiTestHelper.waitForEvents(200, 3, inEventCount, 10000);

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 160L});
        checkStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 50.9F, 110L});

        Assert.assertEquals(inEventCount.get(), 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest6")
    public void updateFromTableTest7() throws InterruptedException {
        //Check for update event data in HBase table when a primary key condition is true.
        log.info("updateFromTableTest7");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2') " +
                "from UpdateStockStream\n" +
                "select symbol, price, volume\n" +
                "update StockTable\n" +
                "set StockTable.volume = volume " +
                "on (StockTable.symbol == symbol);" +
                "@info(name = 'query3') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("query3", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount.incrementAndGet();
                        switch (inEventCount.get()) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 200L});
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});

        updateStockStream.send(new Object[]{"IBM", 30.4F, 200L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 200L});
        SiddhiTestHelper.waitForEvents(200, 2, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

}
