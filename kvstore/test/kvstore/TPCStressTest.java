package kvstore;

import static org.junit.Assert.*;
import static kvstore.KVConstants.*;

import java.net.Socket;

import org.junit.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.powermock.modules.junit4.PowerMockRunner;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.api.mockito.PowerMockito;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Socket.class, KVMessage.class, TPCMasterHandler.class, TPCSlaveInfo.class, TPCMaster.class})
public class TPCStressTest {

    TPCMaster master;
    KVCache masterCache;

    static final int NUMSLAVES = 100;

    static long[] SLAVES = new long[100];

    TPCSlaveInfo slave[] = new TPCSlaveInfo[NUMSLAVES];

    @Before
    public void setupMasterStress() throws KVException {
        for(long i = 0; i <= NUMSLAVES - 1; i++) {
            SLAVES[(int) i] = i;
        }
        masterCache = new KVCache(5, 5);
        master = new TPCMaster(NUMSLAVES, masterCache);

        for(int i = 0; i <= NUMSLAVES - 1; i++) {
            slave[i] = new TPCSlaveInfo(SLAVES[i] + "@111.111.111.111:" + i);
        }
    }

    @Test
    public void testMaxSlavesStress() throws KVException {
        for(long i = 0; i <= NUMSLAVES - 1; i++) {
            SLAVES[(int) i] = i;
        }
        for(int i = 0; i <= NUMSLAVES - 1; i++) {
            master.registerSlave(slave[i]);
        }
        assertTrue(master.getNumRegisteredSlaves() == NUMSLAVES);
    }

    @Test
    public void testFindSuccessorStress() throws KVException {
        for(long i = 0; i <= NUMSLAVES - 1; i++) {
            SLAVES[(int) i] = i;
        }
        for(int i = 0; i <= NUMSLAVES - 1; i++) {
            master.registerSlave(slave[i]);
        }

        for(int i = 0; i <= NUMSLAVES - 2; i++) {
            assertEquals(master.findSuccessor(slave[i]), slave[i + 1]);
        }
        assertEquals(master.findSuccessor(slave[NUMSLAVES - 1]), slave[0]);

    }
    

}