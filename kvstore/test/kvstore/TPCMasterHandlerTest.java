package kvstore;

import static autograder.TestUtils.kTimeoutQuick;
import static kvstore.KVConstants.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import autograder.AGCategories.AGTestDetails;
import autograder.AGCategories.AG_PROJ4_CODE;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TPCMasterHandler.class)
public class TPCMasterHandlerTest {

    private KVServer server;
    private TPCMasterHandler masterHandler;
    private Socket sock1;
    private Socket sock2;
    private Socket sock3;
    private Socket sock4;
    private Socket sock5;
    private Socket sock6;
    private Socket sock7;
    private Socket sock8;

    private static final String LOG_PATH = "TPCMasterHandlerTest.log";

    @Before
    public void setupTPCMasterHandler() throws Exception {
        server = new KVServer(10, 10);
        TPCLog log = new TPCLog(LOG_PATH, server);
        Utils.setupMockThreadPool();
        masterHandler = new TPCMasterHandler( 1L, server, log);
    }

    @After
    public void tearDown() {
        server = null;
        masterHandler = null;
        sock1 = null;
        sock2 = null;
        sock3 = null;
        sock4 = null;
        sock5 = null;
        sock6 = null;
        sock7 = null;
        sock8 = null;
        File log = new File(LOG_PATH);

        if (log.exists() && !log.delete()) { // true iff delete failed.
            System.err.printf("deleting log-file at %s failed.\n", log.getAbsolutePath());
        }
    }

    @Test(timeout = kTimeoutQuick)
    public void testSuccessPut() throws KVException {
        setupSocketSuccess();
        InputStream putreqFile = getClass().getClassLoader().getResourceAsStream("putreq.txt");
        InputStream commitFile = getClass().getClassLoader().getResourceAsStream("commit.txt");
        ByteArrayOutputStream tempOut1 = new ByteArrayOutputStream();
        ByteArrayOutputStream tempOut2 = new ByteArrayOutputStream();
        try {
            doNothing().when(sock1).setSoTimeout(anyInt());
            when(sock1.getInputStream()).thenReturn(putreqFile);
            when(sock1.getOutputStream()).thenReturn(tempOut1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock1);

        try {
            doNothing().when(sock2).setSoTimeout(anyInt());
            when(sock2.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut1.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check1 = new KVMessage(sock2);
        assertEquals(READY, check1.getMsgType());

        try {
            doNothing().when(sock3).setSoTimeout(anyInt());
            when(sock3.getInputStream()).thenReturn(commitFile);
            when(sock3.getOutputStream()).thenReturn(tempOut2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock3);

        try {
            doNothing().when(sock4).setSoTimeout(anyInt());
            when(sock4.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut2.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check2 = new KVMessage(sock4);
        assertEquals(ACK, check2.getMsgType());
    }

    @Test(timeout = kTimeoutQuick)
    public void testFailurePutKey() throws KVException {
        setupSocketSuccess();
        InputStream overKeyPutFile = getClass().getClassLoader().getResourceAsStream("oversizedkeyputreq.txt");
        InputStream abortFile = getClass().getClassLoader().getResourceAsStream("abort.txt");
        ByteArrayOutputStream tempOut1 = new ByteArrayOutputStream();
        ByteArrayOutputStream tempOut2 = new ByteArrayOutputStream();
        try {
            doNothing().when(sock1).setSoTimeout(anyInt());
            when(sock1.getInputStream()).thenReturn(overKeyPutFile);
            when(sock1.getOutputStream()).thenReturn(tempOut1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock1);

        try {
            doNothing().when(sock2).setSoTimeout(anyInt());
            when(sock2.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut1.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check1 = new KVMessage(sock2);
        assertEquals(ABORT, check1.getMsgType());

        try {
            doNothing().when(sock3).setSoTimeout(anyInt());
            when(sock3.getInputStream()).thenReturn(abortFile);
            when(sock3.getOutputStream()).thenReturn(tempOut2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock3);

        try {
            doNothing().when(sock4).setSoTimeout(anyInt());
            when(sock4.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut2.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check2 = new KVMessage(sock4);
        assertEquals(ACK, check2.getMsgType());
    }

    @Test(timeout = kTimeoutQuick)
    public void testFailurePutValue() throws KVException {
        setupSocketSuccess();
        InputStream overValuePutFile = getClass().getClassLoader().getResourceAsStream("oversizedvalueputreq.txt");
        InputStream abortFile = getClass().getClassLoader().getResourceAsStream("abort.txt");
        ByteArrayOutputStream tempOut1 = new ByteArrayOutputStream();
        ByteArrayOutputStream tempOut2 = new ByteArrayOutputStream();
        try {
            doNothing().when(sock1).setSoTimeout(anyInt());
            when(sock1.getInputStream()).thenReturn(overValuePutFile);
            when(sock1.getOutputStream()).thenReturn(tempOut1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock1);

        try {
            doNothing().when(sock2).setSoTimeout(anyInt());
            when(sock2.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut1.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check1 = new KVMessage(sock2);
        assertEquals(ABORT, check1.getMsgType());

        try {
            doNothing().when(sock3).setSoTimeout(anyInt());
            when(sock3.getInputStream()).thenReturn(abortFile);
            when(sock3.getOutputStream()).thenReturn(tempOut2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock3);

        try {
            doNothing().when(sock4).setSoTimeout(anyInt());
            when(sock4.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut2.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check2 = new KVMessage(sock4);
        assertEquals(ACK, check2.getMsgType());
    }
    
    @Test(timeout = kTimeoutQuick)
    public void testSuccessGet() throws KVException {
        setupSocketSuccess();
        InputStream putreqFile = getClass().getClassLoader().getResourceAsStream("putreq.txt");
        InputStream commitFile = getClass().getClassLoader().getResourceAsStream("commit.txt");
        InputStream getreqFile = getClass().getClassLoader().getResourceAsStream("getreq.txt");
        ByteArrayOutputStream tempOut1 = new ByteArrayOutputStream();
        ByteArrayOutputStream tempOut2 = new ByteArrayOutputStream();
        ByteArrayOutputStream tempOut3 = new ByteArrayOutputStream();
        try {
            doNothing().when(sock1).setSoTimeout(anyInt());
            when(sock1.getInputStream()).thenReturn(putreqFile);
            when(sock1.getOutputStream()).thenReturn(tempOut1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock1);

        try {
            doNothing().when(sock2).setSoTimeout(anyInt());
            when(sock2.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut1.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check1 = new KVMessage(sock2);
        assertEquals(READY, check1.getMsgType());

        try {
            doNothing().when(sock3).setSoTimeout(anyInt());
            when(sock3.getInputStream()).thenReturn(commitFile);
            when(sock3.getOutputStream()).thenReturn(tempOut2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock3);

        try {
            doNothing().when(sock4).setSoTimeout(anyInt());
            when(sock4.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut2.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check2 = new KVMessage(sock4);
        assertEquals(ACK, check2.getMsgType());

        try {
            doNothing().when(sock5).setSoTimeout(anyInt());
            when(sock5.getInputStream()).thenReturn(getreqFile);
            when(sock5.getOutputStream()).thenReturn(tempOut3);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock5);

        try {
            doNothing().when(sock6).setSoTimeout(anyInt());
            when(sock3.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut3.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check3 = new KVMessage(sock3);
        assertEquals(RESP, check3.getMsgType());
        assertEquals("key", check3.getKey());
        assertEquals("value", check3.getValue());
    }

    @Test(timeout = kTimeoutQuick)
    public void testFailureGet() throws KVException {
        setupSocketSuccess();
        InputStream getreqFile = getClass().getClassLoader().getResourceAsStream("getreq.txt");
        ByteArrayOutputStream tempOut1 = new ByteArrayOutputStream();
        try {
            doNothing().when(sock1).setSoTimeout(anyInt());
            when(sock1.getInputStream()).thenReturn(getreqFile);
            when(sock1.getOutputStream()).thenReturn(tempOut1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock1);

        try {
            doNothing().when(sock3).setSoTimeout(anyInt());
            when(sock3.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut1.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check1 = new KVMessage(sock3);
        assertEquals(RESP, check1.getMsgType());
        assertEquals(ERROR_NO_SUCH_KEY, check1.getMessage());
    }

    @Test(timeout = kTimeoutQuick)
    public void testSuccessDel() throws KVException {
        setupSocketSuccess();
        InputStream putreqFile = getClass().getClassLoader().getResourceAsStream("putreq.txt");
        InputStream commitFile = getClass().getClassLoader().getResourceAsStream("commit.txt");
        InputStream delreqFile = getClass().getClassLoader().getResourceAsStream("delreq.txt");
        ByteArrayOutputStream tempOut1 = new ByteArrayOutputStream();
        ByteArrayOutputStream tempOut2 = new ByteArrayOutputStream();
        ByteArrayOutputStream tempOut3 = new ByteArrayOutputStream();
        ByteArrayOutputStream tempOut4 = new ByteArrayOutputStream();
        try {
            doNothing().when(sock1).setSoTimeout(anyInt());
            when(sock1.getInputStream()).thenReturn(putreqFile);
            when(sock1.getOutputStream()).thenReturn(tempOut1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock1);

        try {
            doNothing().when(sock2).setSoTimeout(anyInt());
            when(sock2.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut1.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check1 = new KVMessage(sock2);
        assertEquals(READY, check1.getMsgType());

        try {
            doNothing().when(sock3).setSoTimeout(anyInt());
            when(sock3.getInputStream()).thenReturn(commitFile);
            when(sock3.getOutputStream()).thenReturn(tempOut2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock3);

        try {
            doNothing().when(sock4).setSoTimeout(anyInt());
            when(sock4.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut2.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check2 = new KVMessage(sock4);
        assertEquals(ACK, check2.getMsgType());

        try {
            doNothing().when(sock5).setSoTimeout(anyInt());
            when(sock5.getInputStream()).thenReturn(delreqFile);
            when(sock5.getOutputStream()).thenReturn(tempOut3);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock5);

        try {
            doNothing().when(sock6).setSoTimeout(anyInt());
            when(sock6.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut3.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check3 = new KVMessage(sock6);
        assertEquals(READY, check1.getMsgType());

        try {
            doNothing().when(sock7).setSoTimeout(anyInt());
            when(sock7.getInputStream()).thenReturn(commitFile);
            when(sock7.getOutputStream()).thenReturn(tempOut4);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock7);

        try {
            doNothing().when(sock8).setSoTimeout(anyInt());
            when(sock8.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut4.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check4 = new KVMessage(sock8);
        assertEquals(ACK, check2.getMsgType());
    }

    @Test(timeout = kTimeoutQuick)
    public void testFailureDel() throws KVException {
        setupSocketSuccess();
        InputStream delreqFile = getClass().getClassLoader().getResourceAsStream("delreq.txt");
        InputStream abortFile = getClass().getClassLoader().getResourceAsStream("abort.txt");
        ByteArrayOutputStream tempOut1 = new ByteArrayOutputStream();
        ByteArrayOutputStream tempOut2 = new ByteArrayOutputStream();
        try {
            doNothing().when(sock1).setSoTimeout(anyInt());
            when(sock1.getInputStream()).thenReturn(delreqFile);
            when(sock1.getOutputStream()).thenReturn(tempOut1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock1);

        try {
            doNothing().when(sock2).setSoTimeout(anyInt());
            when(sock2.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut1.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check1 = new KVMessage(sock2);
        assertEquals(ABORT, check1.getMsgType());

        try {
            doNothing().when(sock3).setSoTimeout(anyInt());
            when(sock3.getInputStream()).thenReturn(abortFile);
            when(sock3.getOutputStream()).thenReturn(tempOut2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock3);

        try {
            doNothing().when(sock4).setSoTimeout(anyInt());
            when(sock4.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut2.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check2 = new KVMessage(sock4);
        assertEquals(ACK, check2.getMsgType());
    }

    @Test(timeout = kTimeoutQuick)
    public void floatingCommitAbort() throws KVException {
        //Should send an ack if it gets a commit or abort but the last log entry isn't a put or del.
        setupSocketSuccess();
        InputStream commitFile = getClass().getClassLoader().getResourceAsStream("commit.txt");
        InputStream abortFile = getClass().getClassLoader().getResourceAsStream("abort.txt");
        ByteArrayOutputStream tempOut1 = new ByteArrayOutputStream();
        ByteArrayOutputStream tempOut2 = new ByteArrayOutputStream();
        try {
            doNothing().when(sock1).setSoTimeout(anyInt());
            when(sock1.getInputStream()).thenReturn(commitFile);
            when(sock1.getOutputStream()).thenReturn(tempOut1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock1);

        try {
            doNothing().when(sock2).setSoTimeout(anyInt());
            when(sock2.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut1.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check1 = new KVMessage(sock2);
        assertEquals(ACK, check1.getMsgType());

        try {
            doNothing().when(sock3).setSoTimeout(anyInt());
            when(sock3.getInputStream()).thenReturn(abortFile);
            when(sock3.getOutputStream()).thenReturn(tempOut2);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        masterHandler.handle(sock3);

        try {
            doNothing().when(sock4).setSoTimeout(anyInt());
            when(sock4.getInputStream()).thenReturn(new ByteArrayInputStream(tempOut2.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KVMessage check2 = new KVMessage(sock4);
        assertEquals(ACK, check2.getMsgType());
    }


    /* begin helper methods. */

    private void setupSocketSuccess() {
        sock1 = mock(Socket.class);
        sock2 = mock(Socket.class);
        sock3 = mock(Socket.class);
        sock4 = mock(Socket.class);
        sock5 = mock(Socket.class);
        sock6 = mock(Socket.class);
        sock7 = mock(Socket.class);
        sock8 = mock(Socket.class);
    }

}
