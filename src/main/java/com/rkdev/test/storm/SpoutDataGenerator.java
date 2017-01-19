package com.rkdev.test.storm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class SpoutDataGenerator extends BaseRichSpout {
	

	private final static Logger LOGGER = Logger.getLogger(SpoutDataGenerator.class.getName());
	
	private SpoutOutputCollector spoutOutputCollector = null;
	
	private int delayInMiliSeconds = 0;
	
	private int sizeInMBs = 0;
	
	private byte []data = null;
	
	private String name = null;
	
	private ConcurrentHashMap<UUID,Values> pending = new ConcurrentHashMap<>();
	
	private void log(String msg){
		LOGGER.info(name + " : "+ msg);
	}
	
	SpoutDataGenerator(String name,int delayInMiliSeconds,int sizeInMBs){
		this.name = name;
		this.delayInMiliSeconds = delayInMiliSeconds;
		this.sizeInMBs = sizeInMBs;
		int arrayListSize = sizeInMBs*1024*1024;
		data = new byte[arrayListSize];
		for(int i=0;i<data.length;i++){
			data[i]=10;
		}
		log("Created SpoutDataGenerator with delay " + delayInMiliSeconds + "ms and size "+ sizeInMBs + " MB");
	}

	@Override
	public void nextTuple() {
		Utils.sleep(delayInMiliSeconds);
		Values values = new Values(data,System.currentTimeMillis(),System.currentTimeMillis());
		UUID msgId = UUID.randomUUID();
		this.pending.put(msgId,values);
		spoutOutputCollector.emit(values,msgId);
	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		this.spoutOutputCollector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		declare.declare(new Fields("data","timestamp","start-time"));
	}
	
	@Override
	public void ack(Object msgId) {
		this.pending.remove(msgId);
	}

	@Override
	public void fail(Object msgId) {
		this.spoutOutputCollector.emit(this.pending.get(msgId));
	}


}
