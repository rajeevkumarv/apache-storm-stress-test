package com.rkdev.test.storm;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class BoltCustomData extends BaseRichBolt {

	private final static Logger LOGGER = Logger.getLogger(BoltCustomData.class.getName());
	
	public BoltCustomData(String name, int boltEmitSizeInMBs,boolean targetBoltExits) {
		// TODO Auto-generated constructor stub
	}
	
	@Override
	public void execute(Tuple input) {
		MyData mydata = (MyData)input.getValueByField("data");
		LOGGER.info("Data is " + mydata.name);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
	}

}
