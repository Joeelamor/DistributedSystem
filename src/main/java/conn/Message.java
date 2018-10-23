package conn;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Message defines the communication protocol between the Conn interface. It
 * carries a Serializable dataload field that can be used to transfer any
 * serializable object.
 */
class Message implements Serializable {

  private static AtomicLong SequenceCounter = new AtomicLong();
  Type type;
  Serializable dataload;
  private long seq;
  private int senderId;
  private int originatorId;
  private int receiverId;

  public Message(Type type, int senderId, Serializable dataload) {
    this.seq = SequenceCounter.getAndIncrement();
    this.type = type;
    this.senderId = senderId;
    this.originatorId = senderId;
    this.dataload = dataload;
  }

  public Message(Type type, int senderId, int originatorId, int receiverId, Serializable dataload) {
    this.seq = SequenceCounter.getAndIncrement();
    this.type = type;
    this.senderId = senderId;
    this.originatorId = originatorId;
    this.receiverId = receiverId;
    this.dataload = dataload;
  }

  @Override
  public String toString() {
    return String.format("%s\tMessage { sender=%02d, origin=%02d, target=%02d, dataload=%s, seq=%d }",
      type, senderId, originatorId, receiverId, dataload, seq);
  }

  public long getSeq() {
    return seq;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public int getSenderId() {
    return senderId;
  }

  public void setSenderId(int senderId) {
    this.senderId = senderId;
  }

  public int getReceiverId() {
    return receiverId;
  }

  public void setReceiverId(int receiverId) {
    this.receiverId = receiverId;
  }

  public int getOriginatorId() {
    return originatorId;
  }

  public void setOriginatorId(int originatorId) {
    this.originatorId = originatorId;
  }

  public Serializable getDataload() {
    return dataload;
  }

  public void setDataload(Serializable dataload) {
    this.dataload = dataload;
  }

  public enum Type {
    INIT, HELLO, TC, ACK, BCAST
  }
}
