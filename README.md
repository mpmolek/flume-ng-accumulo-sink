flume-ng-accumulo-sink
======================

An Accumulo sink for Flume-NG.

This sink tries to follow the style of the flume-ng-hbase-sink (org.apache.flume.sink.habse)

The AccumuloSink class handles talking to Flume and Accumulo, and an implementation of the AccumuloEventSerializer interface is used to actually generate Accumulo Mutations from Flume events. 
