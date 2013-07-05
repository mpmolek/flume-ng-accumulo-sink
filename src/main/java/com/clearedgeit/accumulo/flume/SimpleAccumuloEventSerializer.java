package com.clearedgeit.accumulo.flume;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.io.Text;

/**
 * Simple implemention of the AccumuloEventSerializer interface.
 * 
 * rowID, columnFamily, and columnVisibility can be set by adding them to the event headers, 
 * either at the source, or with a flume interceptor. If none of those are set, the rowID 
 * will be a random UUID, columnFamily will be "flume" and columnVisibility will be empty.
 */

public class SimpleAccumuloEventSerializer implements AccumuloEventSerializer {
  
  private Event currentEvent;
  
  @Override
  public void configure(Context arg0) {
    // nothing to configure in this simple implementation
  }
  
  @Override
  public void configure(ComponentConfiguration arg0) {
    // nothing to configure in this simple implementation
  }
  
  @Override
  public void set(Event event) {
    this.currentEvent = event;
  }
  
  // In this implementation, this method always
  // returns a list with one Mutation in it.
  // The AccumuloEventSerializer interface
  // leaves the option open for other serializers
  // to do more complicated things.
  @Override
  public List<Mutation> getMutations() {
    
    List<Mutation> mutationList = new LinkedList<Mutation>();
    
    Map<String,String> headers = this.currentEvent.getHeaders();
    
    // If rowID, columnVisibility, or columnFamily are set in the headers
    // of the event, those values will be used (and removed from the headers
    // map so they don't get written to accumulo as actual values.
    // Otherwise, defaults will be used.
    String rowIDHeader = null;
    String visHeader = null;
    String cfHeader = null;
    if (headers != null) {
      if (headers.containsKey("rowID")) {
        rowIDHeader = headers.remove("rowID");
      }
      if (headers.containsKey("columnVisibility")) {
        visHeader = headers.remove("columnVisibility");
      }
      if (headers.containsKey("columnFamily")) {
        cfHeader = headers.remove("columnFamily");
      }
    }
    
    Text rowID = new Text();
    Text cf = new Text();
    ColumnVisibility cv = null;
    
    if (cfHeader != null && cfHeader.length() > 0) {
      cf.set(cfHeader);
    } else {
      cf.set("flume");
    }
    
    if (visHeader != null && visHeader.length() > 0) {
      cv = new ColumnVisibility(visHeader.getBytes());
    } else {
      cv = new ColumnVisibility();
    }
    
    if (rowIDHeader != null && rowIDHeader.length() > 0) {
      rowID.set(rowIDHeader);
    } else {
      rowID.set(UUID.randomUUID().toString());
    }
    
    Mutation mutation = new Mutation(rowID);
    
    // this will write the body with columnQualifier "body" and any remaining headers
    // with columnQualifier "header_"+headerKey
    Map<String,byte[]> entryMap = new HashMap<String,byte[]>();
    entryMap.put("body", currentEvent.getBody());
    
    if (headers != null) {
      for (Map.Entry<String,String> entry : headers.entrySet()) {
        entryMap.put("header_" + entry.getKey(), entry.getValue().getBytes());
      }
    }
    
    Value value = new Value();
    Text cq = new Text();
    for (Entry<String,byte[]> attr : entryMap.entrySet()) {
      cq.set(attr.getKey());
      value.set(attr.getValue());
      mutation.put(cf, cq, cv, value);
    }
    mutationList.add(mutation);
    
    return mutationList;
  }
  
  @Override
  public void close() {
    this.currentEvent = null;
  }
  
}
