package com.eric.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 定义一个bolt继承BaseRichBolt，重写里面的所有方法
 * 每个bolt代码，同样是发送到worker进程的executor线程中的某个task中执行
 * @author pxl
 *
 */
public class SplitSentenceBolt extends BaseRichBolt {

	private static final long serialVersionUID = -9141032617454038741L;
	
	/** bolt的tuple发射器，用于发射数据 */
	private OutputCollector collector;

	@Override
	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		String[] words = sentence.split(" ");
		for(String word : words) {
			collector.emit(new Values(word));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
}