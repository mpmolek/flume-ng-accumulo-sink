package com.clearedgeit.accumulo.flume;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.DefaultSinkFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Testing to make sure the AccumuloSink can connect to Accumulo and that it makes the correct inserts.
 */

public class AccumuloSinkTest {
  
  private SinkFactory sinkFactory;
  private Instance mockInstance;
  
  @Before
  public void setup() {
    sinkFactory = new DefaultSinkFactory();
    mockInstance = new MockInstance("test-instance");
  }
  
  @After
  public void teardown() {
    sinkFactory = null;
    mockInstance = null;
  }
  
  @Test
  public void testSinkCreation() {
    Class<?> typeClass = AccumuloSink.class;
    Sink sink = sinkFactory.create("accumulo-sink", "com.clearedgeit.accumulo.flume.AccumuloSink");
    Assert.assertNotNull(sink);
    Assert.assertTrue(typeClass.isInstance(sink));
  }
  
  /**
   * Connect the AccumuloSink to an Accumulo MockInstance, process an event and make sure it was correctly written to Accumulo.
   * 
   * @throws Exception
   */
  @Test
  public void testProcess() throws Exception {
    
    String tableName = "test_table";
    
    if (mockInstance == null) {
      fail("mockInstance was null");
    }
    
    // create the table that the sink will write to
    Connector conn = mockInstance.getConnector("user", "pass".getBytes());
    conn.tableOperations().create(tableName);
    
    Context sinkContext = new Context();
    sinkContext.put(AccumuloSinkConfigurationConstants.CONFIG_TABLE, tableName);
    
    // create the sink, send it an event, and then close the sink
    AccumuloSink sink = new AccumuloSink(conn);
    Configurables.configure(sink, sinkContext);
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    
    // create an event with body "test event"
    String eventBody = "test event";
    Event event = EventBuilder.withBody(eventBody.getBytes());
    
    // add a couple of headers to the event
    Map<String,String> headers = new HashMap<String,String>();
    
    String headerKey1 = "host";
    String headerValue1 = "host1";
    headers.put(headerKey1, headerValue1);
    
    String headerKey2 = "foo";
    String headerValue2 = "bar";
    headers.put(headerKey2, headerValue2);
    
    event.setHeaders(headers);
    
    // send the event out on the channel, have the sink process it
    // and stop the sink
    channel.put(event);
    tx.commit();
    tx.close();
    sink.process();
    sink.stop();
    
    // scan the table to verify that the correct
    // data made it into accumulo
    
    Scanner scanner = conn.createScanner(tableName, new Authorizations());
    
    String[] rowids = {"", "", ""};
    boolean setBody = false;
    boolean setHeader1 = false;
    boolean setHeader2 = false;
    int count = 0;
    for (Entry<Key,Value> entry : scanner) {
      
      if (count > 2) {
        fail("Too many items in result set");
      }
      
      Key currentKey = entry.getKey();
      String row = currentKey.getRow().toString();
      String cf = currentKey.getColumnFamily().toString();
      String cq = currentKey.getColumnQualifier().toString();
      String vis = currentKey.getColumnVisibility().toString();
      
      // SimpleAccumuloEventSerializer does not set a visibility
      // so it should be an empty string.
      if (!vis.equals("")) {
        fail("Incorrect visibility: " + vis);
      }
      
      // There should be
      if (cf.equals("flume") && cq.equals("body")) {
        Assert.assertArrayEquals(eventBody.getBytes(), entry.getValue().get());
        setBody = true;
        rowids[0] = row;
      } else if (cf.equals("flume") && cq.equals("header_" + headerKey1)) {
        Assert.assertArrayEquals(headerValue1.getBytes(), entry.getValue().get());
        setHeader1 = true;
        rowids[1] = row;
      } else if (cf.equals("flume") && cq.equals("header_" + headerKey2)) {
        Assert.assertArrayEquals(headerValue2.getBytes(), entry.getValue().get());
        setHeader2 = true;
        rowids[2] = row;
      } else {
        System.out.println("UNEXPECTED ENTRY: Key::: " + entry.getKey().toString() + " Value::: " + new String(entry.getValue().get()));
        fail("Unexpected table entry");
      }
      
      count++;
    }
    
    // make sure all three values were found
    Assert.assertTrue("event body was not set in accumulo", setBody);
    Assert.assertTrue("event header1 was not set in accumulo", setHeader1);
    Assert.assertTrue("event header2 was not set in accumulo", setHeader2);
    
    // all three row ids should be the same since these are all part of the
    // same event.
    if (!rowids[0].equals(rowids[1]) || !rowids[0].equals(rowids[2])) {
      fail("row ids were not equal");
    }
    
    // delete the table
    conn.tableOperations().delete(tableName);
  }
  
}
