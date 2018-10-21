package conn;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Receiver implements Runnable {
    private ObjectInputStream inputStream;
    private ConcurrentLinkedQueue<Serializable> queue;

    public Receiver(ObjectInputStream inputStream, ConcurrentLinkedQueue<Serializable> queue) {
        this.inputStream = inputStream;
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Serializable message = (Serializable) this.inputStream.readObject();
                queue.offer(message);
            }
        } catch (IOException e) {
            System.err.println("input stream closed by other end.");
        } catch (ClassNotFoundException e) {
            System.err.println("input object class not found.");
        }
    }
}
