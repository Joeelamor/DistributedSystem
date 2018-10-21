package conn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SimpleConn implements Conn {
    private ConcurrentHashMap<Integer, Sender> senderMap;
    private ConcurrentLinkedQueue<Serializable> messageQueue;
    private int nodeId;

    public SimpleConn(int nodeId, int port) throws IOException {
        ServerSocket listener = new ServerSocket(port);

        this.nodeId = nodeId;
        this.senderMap = new ConcurrentHashMap<>();
        this.messageQueue = new ConcurrentLinkedQueue<>();

        new Thread(new Listener(listener)).start();
    }

    private class Listener implements Runnable {
        private ServerSocket serverSocket;

        private Listener(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                    Message message = (Message) inputStream.readObject();

                    Sender sender = new Sender(outputStream);
                    Thread senderThread = new Thread(sender);
                    senderThread.start();
                    senderMap.put(message.getSenderId(), sender);

                    System.out.println(message);
                    new Thread(new Receiver(inputStream, messageQueue)).start();
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Unable to start server logic");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.err.println("Class of a serialized object cannot be found");
            }
        }
    }

    public void connect(int targetId, String host, int port) throws IOException {
        Socket socket = null;
        int retry = 10;
        while (retry > 0) {
            try {
                socket = new Socket(host, port);
                break;
            } catch (IOException e) {
                retry--;
                if (retry == 0)
                    throw e;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
        ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
        ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
        System.out.println(inputStream);

        Sender sender = new Sender(outputStream);
        Thread senderThread = new Thread(sender);
        senderThread.start();
        senderMap.put(targetId, sender);

        Message message = new Message(nodeId, null);
        this.send(targetId, message);

        new Thread(new Receiver(inputStream, messageQueue)).start();
        System.out.println("Connected an exited host");
    }

    @Override
    public void send(int id, Serializable message) {
        senderMap.get(id).send(message);
    }

    @Override
    public void broadcast(Serializable message) {
        for (Sender sender : senderMap.values()) {
            sender.send(message);
        }
    }

    public Serializable getMessage() {
        while (true) {
            if (messageQueue.isEmpty())
                continue;
            return messageQueue.poll();
        }

    }
}
