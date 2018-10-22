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
    private Long MPRSelectorsSeq;

    TCDataload(HashSet<Integer> MPRSelectors, Long MPRSelectorsSeq) {
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
      sendHelloMsg();
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
      sendTopologyControlMsg();
    }
  }

  private HashSet<Integer> OneHopNeighbors;
  private HashMap<Integer, HashSet<Integer>> PotentialTwoHopNeighbor;

  private HashSet<Integer> MultiPointRelays;
  private long MultiPointRelaysSeq;

  private HashSet<Integer> MPRSelectors;
  private long MPRSelectorsSeq;

  private HashMap<Integer, Pair<HashSet<Integer>, Long>> TopologyTable;
  private HashMap<Integer, Pair<Integer, Integer>> RoutingTable;

  private ConcurrentLinkedQueue<Serializable> messageQueue;

  public LSRConn(int nodeId, int port) throws IOException {
    super(nodeId, port);
    OneHopNeighbors = new HashSet<>();
    PotentialTwoHopNeighbor = new HashMap<>();
    MultiPointRelays = new HashSet<>();
    MultiPointRelaysSeq = 0L;
    MPRSelectors = new HashSet<>();
    MPRSelectorsSeq = 0L;
    TopologyTable = new HashMap<>();
    RoutingTable = new HashMap<>();
    messageQueue = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void connect(Map<Integer, Pair<String, Integer>> connectionList) throws IOException {

    super.connect(connectionList);

    OneHopNeighbors.addAll(connectionList.keySet());

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
          Message message = LSRConn.super.nextMessage();
          switch (message.type) {
            case DATA:
              LSRConn.this.processRcvdDataMsg(message);
              break;
            case HELLO:
              LSRConn.this.processRcvdHelloMsg(message);
              break;
            case TC:
              LSRConn.this.processRcvdTopologyControlMsg(message);
              break;
            case BROADCAST:
              LSRConn.this.processRcvdBroadcastMsg(message);
              break;
            default:
              break;
          }
        }
      }
    });

    MessageProcessor.start();
  }

  @Override
  public void send(int id, Serializable data) {
    Message message = new Message(Message.Type.DATA, nodeId, nodeId, id, data);
    send(getNextHop(message.getReceiverId()), message);
  }

  @Override
  public void broadcast(Serializable data) {
    Message message = new Message(Message.Type.BROADCAST, nodeId, nodeId, -1, data);
    broadcast(message);
  }

  @Override
  public Serializable getMessage() {
    while (true) {
      if (messageQueue.isEmpty())
        continue;
      return messageQueue.poll();
    }
  }

  private void forward(Message message) {
    message.setSenderId(nodeId);
    send(getNextHop(message.getReceiverId()), message);
  }

  private void forwardBroadcast(Message message) {
    if (this.MPRSelectors.contains(message.getSenderId())) {
      int oldSenderID = message.getSenderId();
      message.setSenderId(this.nodeId);
      broadcast(message, oldSenderID);
    }
  }

  private void selectMPR(HashMap<Integer, HashSet<Integer>> neighbors, HashSet<Integer> twohops) {
    HashSet<Integer> MPR = new HashSet<>();
    while (!twohops.isEmpty()) {
      Integer maxID = -1;
      int maxIntersectionSize = 0;
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
      twohops.removeAll(neighbors.get(maxID));
    }
    this.MultiPointRelays = MPR;
  }

  /**
   * send hello
   */
  private void sendHelloMsg() {
    HashSet<Integer> TwoHopNeighbors = new HashSet<>();
    for (HashSet<Integer> potential : this.PotentialTwoHopNeighbor.values()) {
      for (Integer id : potential) {
        if (id == this.nodeId || this.OneHopNeighbors.contains(id))
          continue;
        TwoHopNeighbors.add(id);
      }
    }

    selectMPR(this.PotentialTwoHopNeighbor, TwoHopNeighbors);

    HelloDataload dataload = new HelloDataload(
      this.OneHopNeighbors,
      this.MultiPointRelays
    );
    Message hello = new Message(
      Message.Type.HELLO,
      this.nodeId,
      this.nodeId,
      -1,
      dataload
    );
    super.broadcast(hello);
  }

  /**
   * send TC
   */
  private void sendTopologyControlMsg() {
    TCDataload dataload = new TCDataload(
      this.MPRSelectors,
      this.MPRSelectorsSeq
    );
    Message tc = new Message(Message.Type.TC, this.nodeId, this.nodeId, -1, dataload);
    super.broadcast(tc);
  }

  private void processRcvdDataMsg(Message data) {
    if (nodeId == data.getReceiverId())
      messageQueue.offer(data.dataload);
    else
      forward(data);
  }

  private void processRcvdBroadcastMsg(Message broadcastMessage) {
    this.messageQueue.offer(broadcastMessage.dataload);
    System.out.println("Received broadcast message: " + broadcastMessage);
    forwardBroadcast(broadcastMessage);
    send(broadcastMessage.getOriginatorId(), "receive from node " + nodeId);
  }

  /**
   * receive hello
   */
  private void processRcvdHelloMsg(Message hello) {
    HelloDataload dataload = (HelloDataload) hello.getDataload();

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
   * receive TC
   */
  private void processRcvdTopologyControlMsg(Message tc) {
    TCDataload dataload = (TCDataload) tc.dataload;
    Long currentSeq = 0L;
    if (TopologyTable.containsKey(tc.getOriginatorId()))
      currentSeq = TopologyTable.get(tc.getOriginatorId()).getRight();

    if (currentSeq >= dataload.MPRSelectorsSeq)
      return;
    this.TopologyTable.put(tc.getSenderId(), new ImmutablePair<>(dataload.MPRSelectors, dataload.MPRSelectorsSeq));
    forwardBroadcast(tc);
    calcRoutingTable();
  }

  private void calcRoutingTable() {
    HashMap<Integer, Pair<HashSet<Integer>, Long>> TopoSnapshot = new HashMap<>(this.TopologyTable);
    HashMap<Integer, Pair<Integer, Integer>> RoutingTable = new HashMap<>();
    HashMap<Integer, Pair<Integer, Integer>> NHopNeighbors = new HashMap<>();
    HashMap<Integer, Pair<Integer, Integer>> NplusOneHopNeighbors;
    for (Integer OneHopNeighbor : OneHopNeighbors)
      NHopNeighbors.put(OneHopNeighbor, new ImmutablePair<>(OneHopNeighbor, 1));
    while (!NHopNeighbors.isEmpty()) {
      RoutingTable.putAll(NHopNeighbors);
      NplusOneHopNeighbors = new HashMap<>();
      for (HashMap.Entry<Integer, Pair<Integer, Integer>> NHopNeighbor : NHopNeighbors.entrySet()) {
        Integer NHopID = NHopNeighbor.getKey();
        if (!this.TopologyTable.containsKey(NHopID))
          continue;
        Pair<Integer, Integer> NHopNextHop = NHopNeighbor.getValue();
        for (Integer NplusOneHopNeighbor : TopoSnapshot.get(NHopID).getLeft()) {
          if (RoutingTable.containsKey(NplusOneHopNeighbor))
            continue;
          NplusOneHopNeighbors.put(
            NplusOneHopNeighbor,
            new ImmutablePair<>(
              NHopNextHop.getLeft(),
              NHopNextHop.getRight() + 1
            )
          );
        }
      }
      NHopNeighbors = NplusOneHopNeighbors;
    }
    this.RoutingTable = RoutingTable;
  }

  private int getNextHop(int target) {
    return this.RoutingTable.get(target).getLeft();
  }
}
