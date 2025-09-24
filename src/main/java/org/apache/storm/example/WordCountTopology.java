package org.apache.storm.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.example.bolts.HistogramBolt;
import org.apache.storm.example.bolts.SplitSentenceBolt;
import org.apache.storm.example.bolts.WordCounterBolt;
import org.apache.storm.example.spouts.RandomJokeSpout;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WordCountTopology extends ConfigurableTopology {
  private static final boolean IS_PROD = false;
  public static void main(String[] args) {
    ConfigurableTopology.start(new WordCountTopology(), args);
  }

  @Override
  public int run(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();
    Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);

    // WordSpout: stream of phrases from a book
    builder.setSpout("random-joke-spout", new RandomJokeSpout(), 1);
    // SplitSentenceBolt: splits each sentence into a stream of words
    builder.setBolt("sentence-split", new SplitSentenceBolt(), 1).shuffleGrouping("random-joke-spout");
    // WordCountBolt: counts the words that are emitted
    builder.setBolt("word-count", new WordCounterBolt(), 1).fieldsGrouping("sentence-split", new Fields("word"));
    // HistogramBolt: merges partial counters into a single (global) histogram
    builder.setBolt("histogram-global", new HistogramBolt(), 1).globalGrouping("word-count");

    // configure spout wait strategy to avoid starving other bolts
    // NOTE: learn more here https://storm.apache.org/releases/current/Performance.html
    conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_STRATEGY,
            "org.apache.storm.policy.WaitStrategyProgressive");
    conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL1_COUNT, 1); // wait after 1 consecutive empty emit
    conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL2_COUNT, 100); // wait after 100 consecutive empty emits
    conf.put(Config.TOPOLOGY_BACKPRESSURE_WAIT_PROGRESSIVE_LEVEL3_SLEEP_MILLIS, 1);

    // run the topology (locally if not production, otherwise submit to nimbus)
    if (!IS_PROD) {
        // submit topology
        conf.setDebug(false);
        try (LocalCluster cluster = new LocalCluster()) {
          cluster.submitTopology("WordCountTopology", conf, builder.createTopology());
          try {
            Thread.sleep(60000);
          } catch (Exception exception) {
            System.out.println("Thread interrupted exception : " + exception);
            LOG.error("Thread interrupted exception : ", exception);
          }
          cluster.killTopology("WordCountTopology");
          return 0;
        } catch (Exception e) {
          return 1;
        }
    } else {
      // submit topology
      conf.setDebug(false);
      return submit("WordCountTopology", conf, builder);
    }
  }
}
