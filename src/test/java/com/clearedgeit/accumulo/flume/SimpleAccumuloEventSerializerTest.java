package com.clearedgeit.accumulo.flume;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Throwables;

/**
 * Testing to make sure the SimpleAccumuloEventSerilizer generates the right mutations for some sample events
 */

public class SimpleAccumuloEventSerializerTest {
  
  AccumuloEventSerializer serializer;
  
  @Before
  public void setUp() throws Exception {
    // setting up the serializer the same way it'll be
    // done in the flume sink
    try {
      Class<? extends AccumuloEventSerializer> clazz = Class.forName("com.clearedgeit.accumulo.flume.SimpleAccumuloEventSerializer").asSubclass(
          AccumuloEventSerializer.class);
      serializer = clazz.newInstance();
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }
  
  @After
  public void tearDown() throws Exception {
    serializer.close();
  }
  
  /**
   * Tests the serializer using an event with default headers.
   */
  @Test
  public void testGetMutationsDefault() {
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
    
    // send the event to the serializer and check that it produces the
    // correct mutations
    serializer.set(event);
    List<Mutation> mutations = serializer.getMutations();
    String rowID, cf, cq, cv, value;
    for (Mutation m : mutations) {
      // confirm that rowID matches the pattern of a UUID
      rowID = new String(m.getRow());
      
      boolean validUUID = false;
      try {
        UUID.fromString(rowID);
        validUUID = true;
      } catch (IllegalArgumentException e) {
        validUUID = false;
      }
      Assert.assertTrue("rowID not a valid UUID", validUUID);
      
      // look at all the column updates to make sure they match what was
      // expected
      int count = 0;
      for (ColumnUpdate update : m.getUpdates()) {
        
        if (count > 2) {
          fail("More column updates than expected");
        }
        
        cf = new String(update.getColumnFamily());
        cq = new String(update.getColumnQualifier());
        cv = new String(update.getColumnVisibility());
        value = new String(update.getValue());
        
        if (cf.equals("flume") && cq.equals("body")) {
          Assert.assertEquals(eventBody, value);
        } else if (cf.equals("flume") && cq.equals("header_" + headerKey1)) {
          Assert.assertEquals(headerValue1, value);
        } else if (cf.equals("flume") && cq.equals("header_" + headerKey2)) {
          Assert.assertEquals(headerValue2, value);
        } else {
          System.out.println("UNEXPECTED COLUMN UPDATE: " + update.toString());
          fail("Unexpected column update");
        }
        
        if (!cv.equals("")) {
          fail("Unexpected columnVisibility value");
        }
        
        count++;
      }
    }
  }

  /**
   * Tests the serializer using an event with custom headers.
   * This makes sure the serializer correctly handles the custom
   * rowID, columnFamily, and columnVisibility that are set in the
   * headers.
   */
  @Test
  public void testGetMutationsCustomHeaders() {
    // create an event with body "test event"
    String eventBody = "test event";
    Event event = EventBuilder.withBody(eventBody.getBytes());
    
    // add headers to the event, including custom rowID, columnFamily and
    // columnVisibility values
    Map<String,String> headers = new HashMap<String,String>();
    
    String headerKey1 = "host";
    String headerValue1 = "host1";
    headers.put(headerKey1, headerValue1);
    
    String headerKey2 = "foo";
    String headerValue2 = "bar";
    headers.put(headerKey2, headerValue2);
    
    String rowIDKey = "rowID";
    String rowIDValue = "123456";
    headers.put(rowIDKey, rowIDValue);
    
    String columnFamilyKey = "columnFamily";
    String columnFamilyValue = "customCF";
    headers.put(columnFamilyKey, columnFamilyValue);
    
    String columnVisibiltyKey = "columnVisibility";
    String columnVisibiltyValue = "public";
    headers.put(columnVisibiltyKey, columnVisibiltyValue);
    
    event.setHeaders(headers);
    
    // send the event to the serializer and check that it produces the
    // correct mutations
    serializer.set(event);
    List<Mutation> mutations = serializer.getMutations();
    String rowID, cf, cq, cv, value;
    for (Mutation m : mutations) {
      // confirm that rowID matches the custom rowID that was set in the
      // header
      rowID = new String(m.getRow());
      Assert.assertEquals("123456", rowID);
      
      // look at all the column updates to make sure they match what was
      // expected
      int count = 0;
      for (ColumnUpdate update : m.getUpdates()) {
        
        if (count > 2) {
          fail("More column updates than expected");
        }
        
        cf = new String(update.getColumnFamily());
        cq = new String(update.getColumnQualifier());
        cv = new String(update.getColumnVisibility());
        value = new String(update.getValue());
        
        if (cf.equals("customCF") && cq.equals("body")) {
          Assert.assertEquals(eventBody, value);
        } else if (cf.equals("customCF") && cq.equals("header_" + headerKey1)) {
          Assert.assertEquals(headerValue1, value);
        } else if (cf.equals("customCF") && cq.equals("header_" + headerKey2)) {
          Assert.assertEquals(headerValue2, value);
        } else {
          System.out.println("UNEXPECTED COLUMN UPDATE...");
          System.out.println("rowID: " + rowID);
          System.out.println("CF: " + cf);
          System.out.println("CQ: " + cq);
          System.out.println("CV: " + cv);
          System.out.println("Value: " + value);
          fail("Unexpected column update");
        }
        
        Assert.assertEquals("public", cv);
        
        count++;
      }
    }
  }
}
