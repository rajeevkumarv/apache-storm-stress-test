# apache-storm-stress-test
This project aims to develop testing topology to understand/evaluate storm's data transmission capabilities. With this project, an evaluater can quickly setup topology with multiple spouts/bolts using JSON configuration without modifying the code.

# Getting Started
This project enables JSON based configuraton to quickly setup spouts and bolts which emit configurable amount of data. This JSON file is available at resources/topology_config.json.

This JSON configuration file has two JSON array sections 'spouts' and 'bolts' to configure spouts and bolts respectively. 

######Configuring Spout: 
```
{ "name":"spout1","emit_size_in_mbs":1,"emit_delay_in_ms":100,"parallel_count_hint":1 }
```
######Configuring Bolt:
```
{ "name":"bolt1","emit_size_in_mbs":1,"parallel_count_hint":1, "shuffle_grouping":"spout1" }
```

######Storm Topology Configuration: 
This JSON file also lets define topology configuration under section 'topology_configuraton'.

####Sample JSON to have a spout and bolt
```
{
 "spouts":[
  { "name":"spout1","emit_size_in_mbs":1,"emit_delay_in_ms":100,"parallel_count_hint":1 }
 ],
 "bolts":[
 { "name":"bolt1","emit_size_in_mbs":1,"parallel_count_hint":1, "shuffle_grouping":"spout1" }
 ],
 "topology_config":[
   {
     "topology.max.spout.pending":30,
     "topology.workers":1,
     "topology.eventlogger.executors":1,
     "topology.acker.executors":null
   }
 ]
}
```

####Build Project and package
mvn clean package

####Submit Topology to a storm cluster
./bin/storm target/storm-start-1.0-SNAPSHOT.jar com.rkdev.test.storm.StormStreamingTest test-topology resources/topology_config.json

#Prerequisite
- Storm Cluster Setup - apache-storm-1.0.2 [Download from here](http://storm.apache.org/downloads.html)
- Maven - 3.3.9  
- Java - JDK 1.8

#Authors
- Rajeev Kumar Verma
