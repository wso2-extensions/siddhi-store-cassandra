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

import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.HOST;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.KEY_SPACE;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.PASSWORD;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.cassandra.utils.CassandraTableTestUtils.USER_NAME;


public class DefineCassandraTableTestCase {


    private static final Logger log = Logger.getLogger(DefineCassandraTableTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeClass
    public static void startTest() {
        log.info("== Casandra Table DEFINITION tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== Cassandra Table DEFINITION tests completed ==");
    }

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        CassandraTableTestUtils.initializeTable();
    }

    @Test(testName = "cassandratabledefinitiontest1", description = "Testing table creation.")
    public void cassandratabledefinitiontest1() throws InterruptedException {
        //Testing table creation
        log.info("cassandratabledefinitiontest1");
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
                "from StockStream   " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT", 57.6F, 100L});
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
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"MSFT", 57.6F, 100L});

        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "cassandradefinitiontest2", description = "Testing table creation.",
            dependsOnMethods = "cassandratabledefinitiontest1")
    public void cassandratabledefinitiontest2() throws InterruptedException {
        //Testing table creation with a primary key field
        log.info("cassandratabledefinitiontest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", table.name=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + HOST + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.4F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT", 57.6F, 100L});
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
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.4F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.4F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    //Will throw an exception
    @Test(testName = "cassandratabledefinitiontest3", description = "Testing table a table " +
            "definition with primary key out of the schema.", dependsOnMethods = "cassandratabledefinitiontest2")
    public void cassandratabledefinitiontest3() throws InterruptedException {
        //Testing table creation with a invalid primary key normal insertion.
        log.info("cassandratabledefinitiontest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", table.name=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + HOST + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.start();
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }


    @Test(testName = "cassandratabledefinitiontest4", description = "testing with no user defined primary keys",
            dependsOnMethods = "cassandratabledefinitiontest3")
    public void cassandratabledefinitiontest4() throws InterruptedException {
        //Testing table creation with an additional HBase configuration property.
        log.info("cassandratabledefinitiontest4");
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
                "from StockStream   " +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.4F, 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT", 57.7F, 100L});
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

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.4F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.7F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.4F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"MSFT", 57.7F, 100L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    //Will throw an exception
    @Test(testName = "cassandratabledefinitiontest5", dependsOnMethods = "cassandratabledefinitiontest4",
            description = "Testing wih a wrong schema to a previously defined schema")
    public void cassandratabledefinitiontest5() throws InterruptedException {
        //Defining a cassandra table without an explicit table name.
        log.info("cassandratabledefinitiontest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long, width double); " +
                "define stream CheckStockStream (symbol string, price float, volume long, width double); " +
                "@Store(type=\"cassandra\"," +
                "keyspace=\"" + KEY_SPACE + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + HOST + "\")" +
                "define table StockTable (symbol string, price float, volume long, width double); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

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
                                Assert.assertEquals(event.getData(),
                                        new Object[]{"WSO2", 55.6F, 100L, 20.34});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(),
                                        new Object[]{"IBM", 75.6F, 100L, 30.65});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(),
                                        new Object[]{"MSFT", 57.6F, 100L, 45.67});
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

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L, 20.34});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L, 30.65});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L, 45.67});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L, 20.34});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L, 30.65});
        checkStockStream.send(new Object[]{"MSFT", 57.6F, 100L, 45.67});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    // to run this test a keyspace with wso2sp need to be created
    @Test(testName = "cassandratabledefinitiontest6", description = "test with default keyspace and table",
            dependsOnMethods = "cassandratabledefinitiontest5")
    public void cassandratabledefinitiontest6() throws InterruptedException {
        //Defining a cassandra table without an explicit column family.
        log.info("cassandratabledefinitiontest6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\"," +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + HOST + "\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ; " +
                "@info(name = 'query2') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";


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
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", 55.6F, 400L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT", 57.6F, 100L});
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
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 400L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 400L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "cassandratabledefinitiontest6")
    public void cassandratabledefinitiontest7() throws InterruptedException {
        //Defining a cassandra table with already existing table.
        log.info("cassandratabledefinitiontest7");
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
                "from StockStream   " +
                "insert into StockTable ;" +
                "@info(name = 'query2') " +
                "from CheckStockStream[(symbol==StockTable.symbol and  volume==StockTable.volume and  " +
                "price==StockTable.price) in StockTable] " +
                "insert into OutStream;";

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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 100L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT", 57.6F, 400L});
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
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 400L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 100L});
        checkStockStream.send(new Object[]{"MSFT", 57.6F, 400L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "cassandratabledefinitiontest7")
    public void cassandraTabledefinitiontest8() throws InterruptedException {
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"cassandra\", table.name=\"" + TABLE_NAME + "\", " +
                "keyspace=\"" + KEY_SPACE + "\", " +
                "username=\"" + USER_NAME + "\", " +
                "password=\"" + PASSWORD + "\", " +
                "cassandra.host=\"" + HOST + "\")" +
                "@IndexBy(\"volume\")" +
                "@PrimaryKey(\"symbol,volume\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;" +
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
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", 75.6F, 300L});
                                break;
                            case 3:
                                Assert.assertEquals(event.getData(), new Object[]{"MSFT", 57.6F, 100L});
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
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 300L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});

        checkStockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        checkStockStream.send(new Object[]{"IBM", 75.6F, 300L});
        checkStockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0,  "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }
}
