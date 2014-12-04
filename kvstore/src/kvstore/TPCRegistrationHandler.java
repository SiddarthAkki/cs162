package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of its methods are blocking.
 */
public class TPCRegistrationHandler implements NetworkHandler {

    private ThreadPool threadpool;
    private TPCMaster master;

    /**
     * Constructs a TPCRegistrationHandler with a ThreadPool of a single thread.
     *
     * @param master TPCMaster to register slave with
     */
    public TPCRegistrationHandler(TPCMaster master) {
        this(master, 1);
    }

    /**
     * Constructs a TPCRegistrationHandler with ThreadPool of thread equal to the
     * number given as connections.
     *
     * @param master TPCMaster to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public TPCRegistrationHandler(TPCMaster master, int connections) {
        this.threadpool = new ThreadPool(connections);
        this.master = master;
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param slave Socket connected to the slave with the request
     */
    @Override
    public void handle(Socket slave) {
        KVMessage message;
        try {
            message = new KVMessage(slave);
        } catch (KVException e) {
            message = new KVMessage(RESP, e.getKVMessage().getMessage());
            try {
                message.sendMessage(slave);
            } catch (KVException f) {}
            try {
                slave.close();
            } catch (IOException g) {}
            return;
        }

    threadpool.addJob(this.new TPCRequestRunnable(message, slave));
    }
    
    private class TPCRequestRunnable implements Runnable {
        private KVMessage request;
        private Socket slave;

        TPCRequestRunnable(KVMessage request, Socket slave) {
            this.request = request;
            this.slave = slave;
        }

        @Override
        public void run() {
            KVMessage response = null;
            try{
                String infoString = request.getMessage();
                TPCSlaveInfo info = new TPCSlaveInfo(infoString);
                master.registerSlave(info);
                response = new KVMessage(RESP, "Successfully registered " + infoString);
                response.sendMessage(slave);
            } catch (KVException e) {}
            try {
                slave.close();
            } catch (IOException e) {}
        }
    }
}
