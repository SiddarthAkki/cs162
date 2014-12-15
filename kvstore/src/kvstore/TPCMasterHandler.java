package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;
/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 */
public class TPCMasterHandler implements NetworkHandler {

    public long slaveID;
    public KVServer kvServer;
    public TPCLog tpcLog;
    public ThreadPool threadpool;

    // implement me

    /**
     * Constructs a TPCMasterHandler with one connection in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log) {
        this(slaveID, kvServer, log, 1);
    }

    /**
     * Constructs a TPCMasterHandler with a variable number of connections
     * in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     * @param connections the number of connections in this slave's ThreadPool
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log, int connections) {
        this.slaveID = slaveID;
        this.kvServer = kvServer;
        this.tpcLog = log;
        this.threadpool = new ThreadPool(connections);
    }

    /**
     * Registers this slave server with the master.
     *
     * @param masterHostname
     * @param server SocketServer used by this slave server (which contains the
     *               hostname and port this slave is listening for requests on
     * @throws KVException with ERROR_INVALID_FORMAT if the response from the
     *         master is received and parsed but does not correspond to a
     *         success as defined in the spec OR any other KVException such
     *         as those expected in KVClient in project 3 if unable to receive
     *         and/or parse message
     */
    public void registerWithMaster(String masterHostname, SocketServer server)
            throws KVException {

        Socket master = null;
        try {
            master = new Socket(masterHostname, REGISTRATION_PORT);
        } catch (IOException e) {
            throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
        }
        String repr = "";
        repr += this.slaveID + "@" + server.getHostname() + ":" + server.getPort();
        KVMessage message = new KVMessage(REGISTER, repr);
        message.sendMessage(master);
        KVMessage response = new KVMessage(master);
        try{
            master.close();
        } catch (IOException e){}
        String masterResponse = response.getMessage();
        String correct = "Successfully registered " + repr;
        if (!masterResponse.equals(correct)){
            throw new KVException(ERROR_INVALID_FORMAT);
        }
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param master Socket connected to the master with the request
     */
    @Override
    public void handle(Socket master) {
        // implement me
        KVMessage message;
        try {
            message = new KVMessage(master);
        } catch (KVException e) {
            message = new KVMessage(ABORT, e.getKVMessage().getMessage());
            try {
                message.sendMessage(master);
            } catch (KVException f) {}
            try {
                master.close();
            } catch (IOException g) {}
            return;
        }

        threadpool.addJob(this.new RequestThread(message, master, this.kvServer, this.tpcLog));

    }

    private class RequestThread implements Runnable {
    private Socket master;
    private KVMessage request;
    private KVServer kvServer;
    private TPCLog tpcLog;

    RequestThread(KVMessage request, Socket master, KVServer kvServer, TPCLog tpcLog) {
        this.request = request;
        this.master = master;
        this.kvServer = kvServer;
        this.tpcLog = tpcLog;
        }

        @Override
        public void run() {
            KVMessage response = null;
            String reqName = request.getMsgType();
            try {
                if (reqName.equals(DEL_REQ)) {
                    this.tpcLog.appendAndFlush(request);
                    if (this.kvServer.hasKey(request.getKey())) {
                        response = new KVMessage(READY);
                    } else {
                        response = new KVMessage(ABORT, ERROR_NO_SUCH_KEY);
                    }

                } else if (reqName.equals(GET_REQ)) {
                    String keyval = this.kvServer.get(request.getKey());
                    response = new KVMessage(RESP);
                    response.setKey(request.getKey());
                    response.setValue(keyval);

                } else if (reqName.equals(PUT_REQ)) {
                    this.tpcLog.appendAndFlush(request);
                    try {
                        this.kvServer.keyValueCheck(request.getKey(), request.getValue());
                        response = new KVMessage(READY);
                    } catch (KVException e)  {
                        response = new KVMessage(ABORT, e.getMessage());
                    }

                } else if (reqName.equals(COMMIT) || reqName.equals(ABORT)) {
                    KVMessage lastEntry = this.tpcLog.getLastEntry();
                    if (!(lastEntry == null || lastEntry.getMsgType().equals(COMMIT) || lastEntry.getMsgType().equals(ABORT))) {
                        this.tpcLog.appendAndFlush(request);
                        if (reqName.equals(COMMIT)) {
                            try {
                                if (lastEntry.getMsgType().equals(PUT_REQ)) {
                                    this.kvServer.put(lastEntry.getKey(), lastEntry.getValue());
                                } else if (lastEntry.getMsgType().equals(DEL_REQ)) {
                                    this.kvServer.del(lastEntry.getKey());
                                }
                                response = new KVMessage(ACK);
                            } catch (KVException e)  {
                                response = new KVMessage(RESP, e.getMessage());
                            }

                        } else {
                            response = new KVMessage(ACK);
                        }
                    } else {
                        response = new KVMessage(ACK);
                    }
                }
            } catch (KVException e) {
                response = new KVMessage(RESP, e.getKVMessage().getMessage());
            }
            if (response == null)
            {
                //System.out.println("Error: this should never happen");
                try {
                    master.close();
                } catch (IOException e) {}
                return;
            }
            try {
                response.sendMessage(master);
            } catch (KVException e) {}
            try {
                master.close();
            } catch (IOException e) {}
        }
    }
}
