package com.rkdev.test.storm;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SpoutCustomData extends BaseRichSpout {

	private String name = null;
	private int delayInMiliSeconds;
	private int sizeInMBs = 0;
	private SpoutOutputCollector spoutOutputCollector = null;
	
	public SpoutCustomData(String name,int delayInMiliSeconds,int sizeInMBs) {
		this.name = name;
		this.delayInMiliSeconds = delayInMiliSeconds;
		this.sizeInMBs = sizeInMBs;
	}
	
	@Override
	public void nextTuple() {
		Utils.sleep(delayInMiliSeconds);
		MyData mydata = new MyData();
		mydata.age = 32;
		mydata.salary = 10000;
		mydata.company = "Ricoh";
		mydata.name = "Rajeev";
		spoutOutputCollector.emit(new Values(mydata));
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector spoutOutputCollector) {
		this.spoutOutputCollector = spoutOutputCollector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		declare.declare(new Fields("data"));
	}

}
