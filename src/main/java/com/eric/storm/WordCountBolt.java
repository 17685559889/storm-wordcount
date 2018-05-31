package com.eric.storm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = 7264106830746794207L;
	
	private static Logger LOGGER = LoggerFactory.getLogger(WordCountBolt.class);
	
	private OutputCollector collector;
	
	private ConcurrentHashMap<String, Long> map;
	
	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		map = new ConcurrentHashMap<String, Long>();
	}
	
	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = map.get(word);
		if(count == null) {
			count = 0L;
		}
		count ++;
		map.put(word, count);
		LOGGER.info("【单词计数】" + word + "出现的次数是" + count); 
		collector.emit(new Values(word, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}
}