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
import org.wso2.extension.siddhi.store.cassandra.exception.CassandraTableException;
import org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.KEY_SPACE;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.USER_NAME;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.getHostIp;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.getPort;

public class UpdateOrInsertIntoCassandraTableTestCase {
    private static final Logger log = Logger.getLogger(UpdateOrInsertIntoCassandraTableTestCase.class);
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
        log.info("== Cassandra Table UPDATE/INSERT tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Cassandra Table UPDATE/INSERT tests completed ==");
    }

    @Test
    public void updateOrInsertTableTest1() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest1");
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
                "@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "   on StockTable.symbol==symbol;" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"GOOG", 10.6F, 100L});
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

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        updateStockStream.send(new Object[]{"GOOG", 10.6F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"GOOG", 10.6F, 100L});
        SiddhiTestHelper.waitForEvents(200, 3, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest1")
    public void updateOrInsertTableTest2() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest2");
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
                "@PrimaryKey(\"symbol,price\")" +
                "@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "on StockTable.symbol==symbol ;" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 57.6F, 400L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 101F, 400L});
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

        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 101F, 100L});

        updateStockStream.send(new Object[]{"WSO2", 101F, 400L});

        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 57.6F, 400L});
        checkStockStream.send(new Object[]{"WSO2", 101F, 400L});

        Thread.sleep(500);

        Assert.assertEquals(inEventCount.get(), 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest2")
    public void updateOrInsertTableTest3() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest3");
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
                "@PrimaryKey(\"symbol,price\")" +
                "@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "on StockTable.symbol==symbol and StockTable.price==price ;" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 57.6F, 100L});
                                break;
                            case 4:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 10F, 100L});
                                break;
                            case 5:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 101F, 400L});
                                break;
                            case 6:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 500F, 50L});
                                break;
                            case 7:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 400F, 70L});
                                break;
                            default:
                                Assert.assertEquals(inEventCount, 7);
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

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 10F, 100L});
        stockStream.send(new Object[]{"WSO2", 101F, 100L});

        updateStockStream.send(new Object[]{"WSO2", 101F, 400L});
        updateStockStream.send(new Object[]{"WSO2", 500F, 50L});
        updateStockStream.send(new Object[]{"WSO2", 400F, 70L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 10F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 101F, 400L});
        checkStockStream.send(new Object[]{"WSO2", 500F, 50L});
        checkStockStream.send(new Object[]{"WSO2", 400F, 70L});

        SiddhiTestHelper.waitForEvents(200, 7, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 7, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest3", expectedExceptions = CassandraTableException.class)
    public void updateOrInsertTableTest4() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest4");
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
                "@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "on StockTable.symbol==symbol and StockTable.price==price ;" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 400L});
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

        updateStockStream.send(new Object[]{"WSO2", 55.6F, 400L});
        updateStockStream.send(new Object[]{"WSO2", 500F, 50L});

        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 55.6F, 400L});
        SiddhiTestHelper.waitForEvents(200, 2, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest4")
    public void updateOrInsertTableTest5() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select * " +
                "update or insert into StockTable " +
                "on StockTable.symbol==symbol;" +
                "" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 55.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 50.6F, 700L});
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
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"IBM", 55.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        updateStockStream.send(new Object[]{"WSO2", 50.6F, 700L});

        checkStockStream.send(new Object[]{"IBM", 55.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 50.6F, 700L});

        SiddhiTestHelper.waitForEvents(200, 3, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest5")
    public void updateOrInsertTableTest6() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest6");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select * " +
                "update or insert into StockTable " +
                "on StockTable.symbol==symbol;" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 55.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 50.6F, 700L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"FB", 150.62F, 800L});
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
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 55.6F, 100L});

        updateStockStream.send(new Object[]{"WSO2", 50.6F, 700L});
        updateStockStream.send(new Object[]{"FB", 150.62F, 800L});

        checkStockStream.send(new Object[]{"IBM", 55.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 50.6F, 700L});
        checkStockStream.send(new Object[]{"FB", 150.62F, 800L});
        Thread.sleep(500);

        Assert.assertEquals(inEventCount.get(), 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }


    @Test(dependsOnMethods = "updateOrInsertTableTest6")
    public void updateOrInsertTableTest7() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest7");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", column.family=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", client.port=\"" + getPort() + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + getHostIp() + "\")" +
                //"@PrimaryKey(\"symbol\")" +
                //"@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select * " +
                "update or insert into StockTable " +
                "on StockTable.symbol==symbol and StockTable.price==price ;" +
                "" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 55.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 700L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"FB", 150.62F, 800L});
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
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 55.6F, 100L});

        updateStockStream.send(new Object[]{"WSO2", 55.6F, 700L});
        updateStockStream.send(new Object[]{"FB", 150.62F, 800L});

        checkStockStream.send(new Object[]{"IBM", 55.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 55.6F, 700L});
        checkStockStream.send(new Object[]{"FB", 150.62F, 800L});

        Assert.assertEquals(inEventCount.get(), 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest7")
    public void updateOrInsertTableTest8() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest8");
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
                "@PrimaryKey(\"symbol,price\")" +
                "@Index(\"volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "set StockTable.volume = volume " +
                "on StockTable.symbol==symbol ;" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 700L});
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

        updateStockStream.send(new Object[]{"WSO2", 101F, 700L});

        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 55.6F, 700L});

        SiddhiTestHelper.waitForEvents(200, 2, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateOrInsertTableTest8")
    public void updateOrInsertTableTest9() throws InterruptedException, SQLException {
        log.info("updateOrInsertTableTest8");
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
        String query = "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update or insert into StockTable " +
                "on StockTable.volume>volume ;" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 100L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 101F, 700L});
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

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});

        updateStockStream.send(new Object[]{"WSO2", 101F, 700L});

        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"WSO2", 101F, 700L});

        SiddhiTestHelper.waitForEvents(200, 3, inEventCount, 10000);

        Assert.assertEquals(inEventCount.get(), 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }
}
