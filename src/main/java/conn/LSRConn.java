package conn;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LSRConn extends SimpleConn {

  private class HelloDataload implements Serializable {
    private HashSet<Integer> OneHopNeighbor;
    private HashSet<Integer> MultiPointRelays;

    HelloDataload(HashSet<Integer> oneHopNeighbor, HashSet<Integer> multiPointRelays) {
      OneHopNeighbor = oneHopNeighbor;
      MultiPointRelays = multiPointRelays;
    }
  }

  private class TCDataload implements Serializable {
    private HashSet<Integer> MPRSelectors;
    private long MPRSelectorsSeq;

    TCDataload(HashSet<Integer> MPRSelectors, long MPRSelectorsSeq) {
      this.MPRSelectors = MPRSelectors;
      this.MPRSelectorsSeq = MPRSelectorsSeq;
    }
  }

  /**
   * Each node should periodically floods {@code HELLO} message for link
   * sensing. The node’s MPR set is included in the {@code HELLO} message.
   * <p>
   * The {@code HelloTask} periodically send out {@code HELLO} messages.
   */
  private class HelloTask extends TimerTask {

    final static long delay = 1000L;
    final static long period = 1000L;

    @Override
    public void run() {
      sendHelloMessage();
    }
  }

  /**
   * Nodes with a non-empty {@code MS} periodically flood their {@code MS}
   * via a {@code TC} message.
   * <p>
   * The {@code TopologyControlTask} periodically send out {@code TC} messages
   * if update needed.
   */
  private class TopologyControlTask extends TimerTask {

    final static long delay = 1000L;
    final static long period = 1000L;

    @Override
    public void run() {
      sendTopologyControlMessage();
    }
  }

  private HashSet<Integer> OneHopNeighbor;
  private HashMap<Integer, HashSet<Integer>> PotentialTwoHopNeighbor;

  private HashSet<Integer> MultiPointRelays;
  private long MultiPointRelaysSeq;

  private HashSet<Integer> MPRSelectors;
  private long MPRSelectorsSeq;

  private HashMap<Integer, Pair<HashSet<Integer>, Long>> TopologyTable;

  private ConcurrentLinkedQueue<Serializable> messageQueue;

  public LSRConn(int nodeId, int port) throws IOException {

    super(nodeId, port);

  }

  @Override
  public void connect(Map<Integer, Pair<String, Integer>> connectionList) throws IOException {

    super.connect(connectionList);

    Timer HelloTimer = new Timer("Link Sensing Timer");
    HelloTimer.scheduleAtFixedRate(
      new HelloTask(),
      HelloTask.delay,
      HelloTask.period
    );

    Timer TopologyControlTimer = new Timer("Topology Control Timer");
    TopologyControlTimer.scheduleAtFixedRate(
      new TopologyControlTask(),
      TopologyControlTask.delay,
      TopologyControlTask.period
    );

    Thread MessageProcessor = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          Message message = (Message) LSRConn.super.getMessage();
          switch (message.type) {
            case DATA:
              LSRConn.this.messageQueue.offer(message.dataload);
              break;
            case HELLO:
              LSRConn.this.receiveHelloMessage(message);
              break;
            case TC:
              LSRConn.this.receiveTopologyControlMessage(message);
              break;
            default:
              break;
          }
        }
      }
    });
  }

  @Override
  public void send(int id, Serializable message) {
    super.send(id, message);
  }

  @Override
  public void broadcast(Serializable message) {
    super.broadcast(message);
  }

  @Override
  public Serializable getMessage() {
    return super.getMessage();
  }

  private HashSet<Integer> selectMPR(HashMap<Integer, HashSet<Integer>> neighbors, HashSet<Integer> twohops) {
    HashSet<Integer> MPR = new HashSet<>();
    while (!twohops.isEmpty()) {
      Integer maxID = -1;
      Integer maxIntersectionSize = 0;
      for (HashMap.Entry<Integer, HashSet<Integer>> neighbor : neighbors.entrySet()) {
        if (MPR.contains(neighbor.getKey()))
          continue;
        HashSet<Integer> intersection = new HashSet<>(neighbor.getValue());
        intersection.retainAll(twohops);
        if (intersection.size() > maxIntersectionSize) {
          maxID = neighbor.getKey();
          maxIntersectionSize = intersection.size();
        }
      }
      MPR.add(maxID);
      twohops.retainAll(neighbors.get(maxID));
    }
    return MPR;
  }

  /**
   * send hello
   */
  private void sendHelloMessage() {

    HashSet<Integer> TwoHopNeighbors = new HashSet<>();
    for (HashSet<Integer> potential : this.PotentialTwoHopNeighbor.values()) {
      for (Integer id : potential) {
        if (id == this.nodeId || this.OneHopNeighbor.contains(id))
          continue;
        TwoHopNeighbors.add(id);
      }
    }

    this.MultiPointRelays = selectMPR(this.PotentialTwoHopNeighbor, TwoHopNeighbors);

    HelloDataload dataload = new HelloDataload(
      this.OneHopNeighbor,
      this.MultiPointRelays
    );
    Message hello = new Message(
      Message.MessageType.HELLO,
      this.nodeId,
      dataload
    );
    super.broadcast(hello);
  }

  /**
   * receive hello
   */
  private void receiveHelloMessage(Message hello) {
    HelloDataload dataload = (HelloDataload) hello.dataload;

    this.PotentialTwoHopNeighbor.put(hello.getSenderId(), dataload.OneHopNeighbor);

    if (dataload.MultiPointRelays.contains(this.nodeId)) {
      if (!this.MPRSelectors.contains(hello.getSenderId())) {
        this.MPRSelectors.add(hello.getSenderId());
        this.MPRSelectorsSeq++;
      }
    } else {
      if (this.MPRSelectors.contains(hello.getSenderId())) {
        this.MPRSelectors.remove(hello.getSenderId());
        this.MPRSelectorsSeq++;
      }
    }
  }

  /**
   * send TC
   */
  private void sendTopologyControlMessage() {
    TCDataload dataload = new TCDataload(
      this.MPRSelectors,
      this.MPRSelectorsSeq
    );
    Message hello = new Message(Message.MessageType.TC, this.nodeId, dataload);
    super.broadcast(hello);
  }

  /**
   * receive TC
   */
  private void receiveTopologyControlMessage(Message tc) {
    TCDataload dataload = (TCDataload) tc.dataload;
    Long currentSeq = this.TopologyTable.getOrDefault(tc.getSenderId(), new ImmutablePair<>(null, 0L)).getRight();
    if (currentSeq > dataload.MPRSelectorsSeq)
      return;
    this.TopologyTable.put(tc.getSenderId(), new ImmutablePair<>(dataload.MPRSelectors, dataload.MPRSelectorsSeq));
    if (this.MPRSelectors.contains(tc.getSenderId())) {
      super.broadcast(tc);
    }
  }
}
