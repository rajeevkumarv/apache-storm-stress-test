package com.rkdev.test.storm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class StormStreamingTest {

	public static void main(String[] args) throws FileNotFoundException, IOException, ParseException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {

		if(args.length < 2){
			System.out.println("Usage <topology name> <Topology Configuration File> <true/false run on local cluser>");
			System.exit(0);
		}

		String topologyName = args[0];

		boolean runOnLocalCluster = false;
		if ( args.length > 2){
			runOnLocalCluster = true;
		}

		File topologyConfigFile = new File(args[1]);

		JSONParser parser = new JSONParser();
		JSONObject jsonDocObject =(JSONObject) parser.parse(new FileReader(topologyConfigFile));
		JSONArray jsonSpoutsConfigObject = (JSONArray) jsonDocObject.get("spouts");
		JSONArray jsonBoltssConfigObject = (JSONArray) jsonDocObject.get("bolts");
		JSONArray jsonStringTopologyConfiguration = (JSONArray) jsonDocObject.get("string_configs");
		JSONArray jsonLongTopologyConfiguration = (JSONArray) jsonDocObject.get("long_configs");

		TopologyBuilder builder = new TopologyBuilder();

		for( Object spoutConfig: jsonSpoutsConfigObject ){
			JSONObject jsonSpoutConfigObject = (JSONObject) spoutConfig;
			String spoutName = (String) jsonSpoutConfigObject.get("name");
			long spoutEmitDelay = (long) jsonSpoutConfigObject.get("emit_delay_in_ms");
			long spoutEmitSizeInMBs = (long) jsonSpoutConfigObject.get("emit_size_in_mbs");
			long parallalizationCount = (long) jsonSpoutConfigObject.get("parallel_count_hint");
			builder.setSpout(spoutName,new SpoutDataGenerator(spoutName,(int)spoutEmitDelay,(int)spoutEmitSizeInMBs),parallalizationCount);
		}

		int addedBoltCount=0;
		for( Object boltConfig: jsonBoltssConfigObject ){
			JSONObject jsonBoltConfigObject = (JSONObject) boltConfig;
			String boltName = (String) jsonBoltConfigObject.get("name");
			long boltEmitSizeInMBs = (long) jsonBoltConfigObject.get("emit_size_in_mbs");
			long parallalizationCount = (long) jsonBoltConfigObject.get("parallel_count_hint");
			String shuffleGroupingKey = (String) jsonBoltConfigObject.get("shuffle_grouping");
			BoltDataGenerator bolt = null;
			if(addedBoltCount+1 == jsonBoltssConfigObject.size()){
				bolt = new BoltDataGenerator(boltName,(int)boltEmitSizeInMBs,false);
			}else{
				bolt = new BoltDataGenerator(boltName,(int)boltEmitSizeInMBs,true);
			}
			builder.setBolt(boltName,bolt,parallalizationCount).shuffleGrouping(shuffleGroupingKey);
			++addedBoltCount;
		}

		if(runOnLocalCluster){
			Config conf = new Config();
			conf.setDebug(true);
			conf.setNumWorkers(5);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}else{
			System.out.println("Deploying on cluser ...");
			Config conf = new Config();
			for(Object obj : jsonStringTopologyConfiguration){
				JSONObject oneConfig = (JSONObject) obj;
				for(Object strObj:oneConfig.keySet()){
					conf.put((String)strObj,oneConfig.get(strObj));
					System.out.println("Setting -> " + (String)strObj+" =  "+ oneConfig.get(strObj));
				}
			}

			for(Object obj : jsonLongTopologyConfiguration){
				JSONObject oneConfig = (JSONObject) obj;
				for(Object strObj:oneConfig.keySet()){
					long val = (long) oneConfig.get(strObj);
					conf.put((String)strObj,val);
					System.out.println("Setting -> "+ (String) strObj + " = "+ val);
				}					
			}				

			//			Config config = new Config(); 
			//		      config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,
			//		      new Integer(16384));
			//		      config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,
			//		      new Integer(16384));
			//		      config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,
			//		      new Integer(32));

			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		}


		//		int spoutEmitSizeInMBs = Integer.parseInt(args[0]);
		//		int spoutEmitDelay = Integer.parseInt(args[1]);
		//		int boltEmitSize = Integer.parseInt(args[2]);
		//		int parallalizationCount = Integer.parseInt(args[3]);
		//		
		//		TopologyBuilder builder = new TopologyBuilder();
		//		builder.setSpout("image_generator",new SpoutDataGenerator(spoutEmitDelay,spoutEmitSizeInMBs));
		//		builder.setBolt("rivr_bolt",new BoltDataGenerator(spoutEmitSizeInMBs)).shuffleGrouping("image_generator");
		//		builder.setBolt("ransac_bolt",new BoltDataGenerator(boltEmitSize)).shuffleGrouping("image_generator");
	}
}
