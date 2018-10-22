package conn;

import java.io.Serializable;

class Message implements Serializable {

  Type type;
  Serializable dataload;
  private int seq;
  private int senderId;
  private int originatorId;
  private int receiverId;
  public Message(Type type, int senderId, Serializable dataload) {
    this.type = type;
    this.senderId = senderId;
    this.dataload = dataload;
  }

  public Message(Type type, int senderId, int originatorId, int receiverId, Serializable dataload) {
    this.type = type;
    this.senderId = senderId;
    this.originatorId = originatorId;
    this.receiverId = receiverId;
    this.dataload = dataload;
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
    INIT, HELLO, TC, DATA, BROADCAST
  }
}
