package com.eric.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * 单词计数拓扑
 * @author pxl
 *
 */
public class WordCountTopology {
	
	public static void main(String[] args) {
		// 在main方法中，将spout和bolts组合起来，构建成一个拓扑
		TopologyBuilder builder = new TopologyBuilder();
		
		// 第三个参数设置spout的executor有几个
		builder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);
		//ShuffleGrouping：随机选择一个Task来发送。
		builder.setBolt("SplitSentence", new SplitSentenceBolt(), 5)
			.setNumTasks(10)
			.shuffleGrouping("RandomSentence");
		
		// 这个很重要，就是说，相同的单词，从SplitSentence发射出来时，一定会进入到下游的指定的同一个task中
		// 只有这样子，才能准确的统计出每个单词的数量
		// 比如你有个单词，hello，下游task1接收到3个hello，task2接收到2个hello
		// 5个hello，全都进入一个task
		//FiledGrouping：根据Tuple中Fields来做一致性hash，相同hash值的Tuple被发送到相同的Task。
		builder.setBolt("WordCount", new WordCountBolt(), 10)
			.setNumTasks(20)
			.fieldsGrouping("SplitSentence", new Fields("word"));
		
		Config config = new Config();
		if(args != null && args.length > 0) {
			// 说明是在命令行执行，打算提交到storm集群上去
			config.setNumWorkers(3);
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			// 说明是在eclipse里面本地运行
			config.setMaxTaskParallelism(3);  
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("WordCountTopology", config, builder.createTopology());  
			Utils.sleep(60000); 
			cluster.shutdown();
		}
	}
	
}
