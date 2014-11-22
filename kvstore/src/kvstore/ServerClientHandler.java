package kvstore;

import static kvstore.KVConstants.DEL_REQ;
import static kvstore.KVConstants.GET_REQ;
import static kvstore.KVConstants.PUT_REQ;
import static kvstore.KVConstants.RESP;
import static kvstore.KVConstants.SUCCESS;

import java.io.IOException;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of its methods are blocking.
 */
public class ServerClientHandler implements NetworkHandler {

    public KVServer kvServer;
    public ThreadPool threadPool;

    /**
     * Constructs a ServerClientHandler with ThreadPool of a single thread.
     *
     * @param kvServer KVServer to carry out requests
     */
    public ServerClientHandler(KVServer kvServer) {
        this(kvServer, 1);
    }

    /**
     * Constructs a ServerClientHandler with ThreadPool of thread equal to
     * the number passed in as connections.
     *
     * @param kvServer KVServer to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public ServerClientHandler(KVServer kvServer, int connections) {
        this.kvServer = kvServer;
        threadPool = new ThreadPool(connections);
    }

    /**
     * Creates a job to service the request for a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param client Socket connected to the client with the request
     */
    @Override
    public void handle(Socket client) {
        KVMessage message;
        try {
            message = new KVMessage(client);
        } catch (KVException e) {
            message = new KVMessage(RESP, e.getKVMessage());
            try {
                message.send(client);
                if (!client.isClosed()) {
                    client.close();
                }
                return;              
            } catch (KVException e) {
                if (!client.isClosed()) {
                    client.close();
                }
                return;
            }
 
        }
        String messType = mess.getMsgType();
        String key = mess.getKey();
    }
    
    private static class RequestThread extends Thread {
        private String key;
        private String value;
        private String reqName;

        RequestThread(String key, String value, String reqName) {
            this.key = key;
            this.value = value;
            this.reqName = reqName;

        }

        public 
    }

}
