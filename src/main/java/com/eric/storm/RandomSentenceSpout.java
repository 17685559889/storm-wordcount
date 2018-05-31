package com.eric.storm;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 集成BaseRichSpout基类
 * 主要负责从数据源获取数据
 * @author pxl
 *
 */
public class RandomSentenceSpout extends BaseRichSpout{

	private static final long serialVersionUID = -232900046072314599L;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RandomSentenceSpout.class);
	
	/** 用于发射数据 */
	private SpoutOutputCollector collector;
	
	private Random random;
	
	/**
	 * open方法
	 * 用于对spout进行初始化
	 * 比如创建线程池,数据库连接池
	 */
	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.random = new Random();
	}
	
	/**
	 * nextTuple方法
	 * 被worker进程的executor线程中的某个task无限循环调用
	 * 不断的发射最新的数据，形成一个数据流
	 */
	@Override
	public void nextTuple() {
		Utils.sleep(2000);
		String[] sentences = new String[]{"the cow jumped over the moon", "an apple a day keeps the doctor away",
				"four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"};
		String sentence = sentences[random.nextInt(sentences.length)];
		LOGGER.info("【发射句子】：" + sentence);
		collector.emit(new Values(sentence));
	}
	
	/**
	 * declareOutputFielfs方法
	 * 定义一发射出去的每个tuple中的每个field的名称是什么
	 * 
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
}