package com.rkdev.test.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class BoltDataGenerator extends BaseRichBolt {

	private final static Logger LOGGER = Logger.getLogger(BoltDataGenerator.class.getName());

	private OutputCollector outputCollector = null;

	private int sizeInMBs = 0;

	private String name = null;

	private boolean targetBoltExist = true;

	byte []data = null;

	private void log(String msg){
		LOGGER.info(name + " : "+ msg);
	}

	public BoltDataGenerator(String name, int boltEmitSizeInMBs,boolean targetBoltExits){
		this.name = name;
		this.sizeInMBs = boltEmitSizeInMBs;
		this.targetBoltExist=targetBoltExits;
		log("Created BoltDataGenerator with size "+ sizeInMBs + " MB");
	}

	@Override
	public void execute(Tuple input) {

		byte[] bytesArray = input.getBinaryByField("data");
		long messageSentTimeInMiliSeconds = (long) input.getValueByField("timestamp");
		long startTime = (long) input.getValueByField("start-time");
		long elapsedTime = System.currentTimeMillis() - messageSentTimeInMiliSeconds;
		double transitTime = System.currentTimeMillis() - startTime;
		log("Message of size "+ bytesArray.length/(1024*1024) + " MBs  received in " + elapsedTime + " milisconds and transit time is "+ transitTime/1000 + " seconds");

		ArrayList<Object> tuple = new ArrayList<>();
		tuple.add(data);
		tuple.add(System.currentTimeMillis());
		tuple.add(startTime);
		if(targetBoltExist){
			outputCollector.emit(input,tuple);
		}
		outputCollector.ack(input);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.outputCollector = arg2; 
		int arrayListSize = sizeInMBs*1024*1024;
		data = new byte[arrayListSize];
		for(int i=0;i<data.length;i++){
			data[i]=10;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		declare.declare(new Fields("data","timestamp","start-time"));
	}

}
