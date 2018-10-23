package conn;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

public class OLSRConn extends SimpleConn {

  private final static long CONVERGENCE_CHECK_INTERVAL = 5000L;

  private int totalNodeNumber;

  private HashSet<Integer> OneHopNeighbors;
  private HashMap<Integer, HashSet<Integer>> PotentialTwoHopNeighbor;
  private HashSet<Integer> MultiPointRelays;
  private long MultiPointRelaysSeq;
  private HashSet<Integer> MPRSelectors;
  private long MPRSelectorsSeq;
  private volatile boolean MPRSelectorSent;
  private HashMap<Integer, Pair<HashSet<Integer>, Long>> TopologyTable;
  private HashMap<Integer, Pair<Integer, Integer>> RoutingTable;
  private HashMap<Integer, Long> LastMsgSeq;
  private ConcurrentLinkedQueue<Serializable> messageQueue;
  private volatile boolean converge;

  private final Semaphore pendingBroadcastACK = new Semaphore(0);

  @Override
  public String toString() {
    return "OLSRConn{" +
      "OneHopNeighbors=" + OneHopNeighbors +
      ", \nPotentialTwoHopNeighbor=" + PotentialTwoHopNeighbor +
      ", \nMultiPointRelays=" + MultiPointRelays +
      ", \nMultiPointRelaysSeq=" + MultiPointRelaysSeq +
      ", \nMPRSelectors=" + MPRSelectors +
      ", \nMPRSelectorsSeq=" + MPRSelectorsSeq +
      ", \nTopologyTable=" + TopologyTable +
      ", \nRoutingTable=" + RoutingTable +
      ", \nmessageQueue=" + messageQueue +
      '}';
  }

  public OLSRConn(int nodeId, int port, int totalNodeNumber) throws IOException {
    super(nodeId, port);
    this.totalNodeNumber = totalNodeNumber;
    OneHopNeighbors = new HashSet<>();
    PotentialTwoHopNeighbor = new HashMap<>();
    MultiPointRelays = new HashSet<>();
    MultiPointRelaysSeq = 0L;
    MPRSelectors = new HashSet<>();
    MPRSelectorsSeq = 0L;
    MPRSelectorSent = true;
    TopologyTable = new HashMap<>();
    RoutingTable = new HashMap<>();
    LastMsgSeq = new HashMap<>();
    messageQueue = new ConcurrentLinkedQueue<>();
    converge = false;
  }

  public HashSet<Integer> getTreeNeighbor() {
    return new HashSet<>(MultiPointRelays);
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
          Message message = OLSRConn.super.nextMessage();
          switch (message.type) {
            case ACK:
              OLSRConn.this.processRcvdACKMsg(message);
              break;
            case HELLO:
              System.out.println("Received\t" + message);
              OLSRConn.this.processRcvdHelloMsg(message);
              break;
            case TC:
              System.out.println("Received\t" + message);
              OLSRConn.this.processRcvdTopologyControlMsg(message);
              break;
            case BCAST:
              OLSRConn.this.processRcvdBroadcastMsg(message);
              break;
            default:
              break;
          }
        }
      }
    });

    MessageProcessor.start();

    while (true) {
      try {
        long oldMPRSeq = MultiPointRelaysSeq;
        Thread.sleep(CONVERGENCE_CHECK_INTERVAL);
        if (oldMPRSeq == MultiPointRelaysSeq) {
          converge = true;
          HelloTimer.cancel();
          TopologyControlTimer.cancel();
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }
  }

  @Override
  public void send(int id, Serializable data) {
    Message message = new Message(Message.Type.ACK, nodeId, nodeId, id, data);
    send(getNextHop(message.getReceiverId()), message);
  }

  @Override
  public synchronized void broadcast(Serializable data) {
    Message message = new Message(Message.Type.BCAST, nodeId, data);
    broadcast(message);

    //down()
    pendingBroadcastACK.acquireUninterruptibly(totalNodeNumber - 1);
  }

  @Override
  public Serializable getMessage() {
    while (true) {
      if (messageQueue.isEmpty())
        continue;
      return messageQueue.poll();
    }
  }

  @Override
  public boolean hasConverged() {
    return converge;
  }

  private void forward(Message message) {
    message.setSenderId(nodeId);
    send(getNextHop(message.getReceiverId()), message);
  }

  private void forwardBroadcast(Message message) {
    if (this.MPRSelectors.contains(message.getSenderId())) {
      int oldSenderID = message.getSenderId();
      message.setSenderId(nodeId);
      System.out.println("Forward \t" + message);
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
    if (!MultiPointRelays.equals(MPR)) {
      MultiPointRelays = MPR;
      MultiPointRelaysSeq++;
    }
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
      dataload
    );
    super.broadcast(hello);
  }

  /**
   * send TC
   */
  private void sendTopologyControlMsg() {
    if (MPRSelectorSent)
      return;
    TCDataload dataload = new TCDataload(
      this.MPRSelectors,
      this.MPRSelectorsSeq
    );
    Message tc = new Message(Message.Type.TC, nodeId, dataload);
    super.broadcast(tc);
    MPRSelectorSent = false;
  }

  private void processRcvdACKMsg(Message message) {
    if (nodeId == message.getReceiverId()) {
      System.out.println("Received\t" + message);
      // up(pendingACK)
      pendingBroadcastACK.release(1);
      messageQueue.offer(message.dataload);
    } else {
      System.out.println("Forward \t" + message);
      forward(message);
    }
  }

  private void processRcvdBroadcastMsg(Message message) {
    int origin = message.getOriginatorId();
    if (LastMsgSeq.getOrDefault(origin, 0L) >= message.getSeq())
      return;
    LastMsgSeq.put(origin, message.getSeq());
    System.out.println("Received\t" + message);
    this.messageQueue.offer(message.dataload);
    forwardBroadcast(message);
    System.out.println("Reply to " + message.getOriginatorId());
    send(message.getOriginatorId(), "receive from node " + nodeId);
  }

  /**
   * receive hello
   */
  private void processRcvdHelloMsg(Message message) {
    HelloDataload dataload = (HelloDataload) message.getDataload();

    PotentialTwoHopNeighbor.put(message.getSenderId(), dataload.OneHopNeighbor);

    if (dataload.MultiPointRelays.contains(nodeId)) {
      if (!MPRSelectors.contains(message.getSenderId())) {
        MPRSelectors.add(message.getSenderId());
        MPRSelectorsSeq++;
        MPRSelectorSent = false;
      }
    } else {
      if (MPRSelectors.contains(message.getSenderId())) {
        MPRSelectors.remove(message.getSenderId());
        MPRSelectorsSeq++;
        MPRSelectorSent = false;
      }
    }
  }

  /**
   * receive TC
   */
  private void processRcvdTopologyControlMsg(Message message) {
    TCDataload dataload = (TCDataload) message.dataload;
    Long currentSeq = 0L;
    if (TopologyTable.containsKey(message.getOriginatorId()))
      currentSeq = TopologyTable.get(message.getOriginatorId()).getRight();

    if (currentSeq >= dataload.MPRSelectorsSeq)
      return;
    TopologyTable.put(message.getOriginatorId(), new ImmutablePair<>(dataload.MPRSelectors, dataload.MPRSelectorsSeq));
    forwardBroadcast(message);
    calcRoutingTable();
  }

  private HashMap<Integer, HashSet<Integer>> getTopologyTableSnapshot() {
    HashMap<Integer, HashSet<Integer>> topology = new HashMap<>();
    topology.put(nodeId, OneHopNeighbors);
    for (Map.Entry<Integer, Pair<HashSet<Integer>, Long>> topoEntry : TopologyTable.entrySet()) {
      topology.put(topoEntry.getKey(), topoEntry.getValue().getLeft());
    }
    return topology;
  }

  private void calcRoutingTable() {
    HashMap<Integer, HashSet<Integer>> TopoSnapshot = getTopologyTableSnapshot();
    HashMap<Integer, Pair<Integer, Integer>> RoutingTable = new HashMap<>();
    HashMap<Integer, Pair<Integer, Integer>> NHopNeighbors = new HashMap<>();
    HashMap<Integer, Pair<Integer, Integer>> NplusOneHopNeighbors;
    NHopNeighbors.put(nodeId, new ImmutablePair<>(nodeId, 0));
    while (!NHopNeighbors.isEmpty()) {
      RoutingTable.putAll(NHopNeighbors);
      NplusOneHopNeighbors = new HashMap<>();
      for (HashMap.Entry<Integer, Pair<Integer, Integer>> NHopNeighbor : NHopNeighbors.entrySet()) {
        Integer NHopID = NHopNeighbor.getKey();
        if (!TopoSnapshot.containsKey(NHopID))
          continue;
        Pair<Integer, Integer> NHopNextHop = NHopNeighbor.getValue();
        for (Integer NplusOneHopNeighbor : TopoSnapshot.get(NHopID)) {
          if (RoutingTable.containsKey(NplusOneHopNeighbor))
            continue;
          NplusOneHopNeighbors.put(
            NplusOneHopNeighbor,
            new ImmutablePair<>(
              NHopNextHop.getLeft() == nodeId ? NplusOneHopNeighbor : NHopNextHop.getLeft(),
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

  static class HelloDataload implements Serializable {
    private HashSet<Integer> OneHopNeighbor;
    private HashSet<Integer> MultiPointRelays;

    HelloDataload(HashSet<Integer> oneHopNeighbor, HashSet<Integer> multiPointRelays) {
      OneHopNeighbor = oneHopNeighbor;
      MultiPointRelays = multiPointRelays;
    }

    @Override
    public String toString() {
      return "HelloDataload[" +
        "OneHopNeighbor=" + OneHopNeighbor +
        ", MultiPointRelays=" + MultiPointRelays +
        ']';
    }
  }

  static class TCDataload implements Serializable {
    private HashSet<Integer> MPRSelectors;
    private Long MPRSelectorsSeq;

    TCDataload(HashSet<Integer> MPRSelectors, Long MPRSelectorsSeq) {
      this.MPRSelectors = MPRSelectors;
      this.MPRSelectorsSeq = MPRSelectorsSeq;
    }

    @Override
    public String toString() {
      return "TCDataload[" +
        "MPRSelectors=" + MPRSelectors +
        ", MPRSelectorsSeq=" + MPRSelectorsSeq +
        ']';
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
    final static long period = 100L;

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
    final static long period = 200L;

    @Override
    public void run() {
      sendTopologyControlMsg();
    }
  }
}
