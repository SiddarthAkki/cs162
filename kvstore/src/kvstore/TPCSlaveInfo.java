package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.*;
import java.util.regex.*;

/**
 * Data structure to maintain information about SlaveServers
 */
public class TPCSlaveInfo {

    public long slaveID;
    public String hostname;
    public int port;

    /**
     * Construct a TPCSlaveInfo to represent a slave server.
     *
     * @param info as "SlaveServerID@Hostname:Port"
     * @throws KVException ERROR_INVALID_FORMAT if info string is invalid
     */
    public TPCSlaveInfo(String info) throws KVException {
        String[] params = info.split("@");
        try {
            this.slaveID = Long.parseLong(params[0]);
            String[] nestedParams = params[1].split(":");
            this.hostname = nestedParams[0];
            String hostnameLower = this.hostname.toLowerCase();
            //hostname labels may contain only the ASCII letters 'a' through 'z' (in a case-insensitive manner), the digits '0' through '9', the hyphen ('-'), and dots. Consecutive hyphens and dots aren't valid.
            boolean seenPunc = false;
            for (int i = 0; i < hostnameLower.length(); i++)
            {
                //first char must be 0-9 or a-z (case-insensitive)
                if (i == 0)
                {
                    if ( (hostnameLower.charAt(i) >= 'a' && hostnameLower.charAt(i) <= 'z') || (hostnameLower.charAt(i) >= '0' && hostnameLower.charAt(i) <= '9'))
                    {
                        
                    }
                    else
                    {
                        throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
                    }
                }
                else
                {
                    if ( (hostnameLower.charAt(i) >= 'a' && hostnameLower.charAt(i) <= 'z') || (hostnameLower.charAt(i) >= '0' && hostnameLower.charAt(i) <= '9'))
                    {
                        seenPunc = false;
                    }
                    else if ((hostnameLower.charAt(i) == '-') || (hostnameLower.charAt(i) == '.'))
                    {
                        if (seenPunc == true)
                        {
                            throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
                        }
                        else
                        {
                            seenPunc = true;
                        }
                    }
                    else
                    {
                        throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
                    }
                }
                
            }
            this.port = Integer.parseInt(nestedParams[1]);
        } catch (NumberFormatException e) {
            throw new KVException(KVConstants.ERROR_INVALID_FORMAT);
        }
    }

    public long getSlaveID() {
        return slaveID;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    /**
     * Create and connect a socket within a certain timeout.
     *
     * @return Socket object connected to SlaveServer, with timeout set
     * @throws KVException ERROR_SOCKET_TIMEOUT, ERROR_COULD_NOT_CREATE_SOCKET,
     *         or ERROR_COULD_NOT_CONNECT
     */
    public Socket connectHost(int timeout) throws KVException {
        try {
            Socket sock = new Socket(this.hostname, this.port);
            sock.setSoTimeout(timeout);
            return sock;
        } catch (SocketException e) {
            throw new KVException(KVConstants.ERROR_SOCKET_TIMEOUT);
        } catch (NullPointerException | SecurityException | IllegalArgumentException e ) {
            throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
        } catch (IOException e) {
            throw new KVException(ERROR_COULD_NOT_CONNECT);
        }
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param sock Socket to be closed
     */
    public void closeHost(Socket sock) {
        try {
            sock.close();
        } catch (IOException e) {}
    }
}
