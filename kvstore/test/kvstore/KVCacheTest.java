package kvstore;

import static org.junit.Assert.*;
import static kvstore.KVConstants.*;


import static autograder.TestUtils.kTimeoutQuick;
import autograder.AGCategories.AGTestDetails;
import autograder.AGCategories.AG_PROJ3_CODE;

import org.junit.*;
import org.junit.experimental.categories.Category;

public class KVCacheTest {

    /**
     * Verify the cache can put and get a KV pair successfully.
     */
    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void singlePutAndGet() {
        KVCache cache = new KVCache(1, 4);
        cache.put("hello", "world");
        //System.out.println(cache.toXML());
        assertEquals("world", cache.get("hello"));
        assertEquals(cache.getCacheSetSize(0), 1);
    }

    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void fourPutsandGets() {
        KVCache cache = new KVCache(1, 4);
        cache.put("1", "hello");
        cache.put("2", "iris");
        cache.put("3", "is");
        cache.put("4", "awesomeeeeee!");
        assertEquals("hello", cache.get("1"));
        assertEquals("iris", cache.get("2"));
        assertEquals("is", cache.get("3"));
        assertEquals("awesomeeeeee!", cache.get("4"));
        assertEquals(cache.getCacheSetSize(0), 4);
        //System.out.println(cache.toXML());
    }

    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void repeatedPuts() {
        KVCache cache = new KVCache(1, 4);
        cache.put("1", "hello");
        //System.out.println(cache.toXML());
        assertEquals("hello", cache.get("1"));
        cache.put("1", "is");
        //System.out.println(cache.toXML());
        assertEquals("is", cache.get("1"));
        assertEquals(cache.getCacheSetSize(0), 1);
    }

    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void singlePutAndDel() {
        KVCache cache = new KVCache(1, 4);
        cache.put("hello", "world");
        assertEquals("world", cache.get("hello"));
        cache.del("hello");
        assertEquals(null, cache.get("hello"));
        assertEquals(cache.getCacheSetSize(0), 0);
    }

    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void twoPutsAndDels() {
        KVCache cache = new KVCache(1, 4);
        cache.put("1", "hello");
        cache.put("2", "iris");
        cache.put("3", "is");
        cache.put("4", "awesomeeeeee!");
        assertEquals("hello", cache.get("1"));
        assertEquals("iris", cache.get("2"));
        assertEquals("is", cache.get("3"));
        assertEquals("awesomeeeeee!", cache.get("4"));
        //System.out.println(cache.toXML());

        cache.del("1");
        assertEquals(null, cache.get("1"));
        //System.out.println(cache.toXML());

        cache.del("2");
        assertEquals(null, cache.get("2"));
        //System.out.println(cache.toXML());

        cache.del("3");
        assertEquals(null, cache.get("3"));
        //System.out.println(cache.toXML());

        cache.del("4");
        assertEquals(null, cache.get("4"));
        //System.out.println(cache.toXML());
        assertEquals(cache.getCacheSetSize(0), 0);
    }

    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void tooManyPuts() {
        KVCache cache = new KVCache(1, 1);
        cache.put("hello", "world");
        //System.out.println(cache.toXML());
        assertEquals("world", cache.get("hello"));
        cache.put("foo", "bar");
        assertEquals(null, cache.get("hello"));
        assertEquals("bar", cache.get("foo"));
        assertEquals(cache.getCacheSetSize(0), 1);
        //System.out.println(cache.toXML());
    }

    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void tooManyPutsWithGets() {
        KVCache cache = new KVCache(1, 4);
        cache.put("one", "hello");
        cache.put("two", "iris");
        cache.put("three", "is");
        cache.put("four", "awesomeeeeee!");
        assertEquals("hello", cache.get("one"));
        assertEquals("iris", cache.get("two"));
        assertEquals("is", cache.get("three"));
        assertEquals("awesomeeeeee!", cache.get("four"));
        cache.put("five", "YAY");
        //System.out.println(cache.toXML());

        assertEquals(null, cache.get("one"));
        assertEquals("iris", cache.get("two"));
        assertEquals("is", cache.get("three"));
        assertEquals("awesomeeeeee!", cache.get("four"));
        assertEquals("YAY", cache.get("five"));
        assertEquals(cache.getCacheSetSize(0), 4);
        //System.out.println(cache.toXML());
    }

    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void multipleSetPuts() {
        KVCache cache = new KVCache(2, 4);
        cache.put("1", "hi");
        cache.put("2", "there");
        assertEquals("hi", cache.get("1"));
        assertEquals("there", cache.get("2"));
        assertEquals(cache.getCacheSetSize(0), 1);
        assertEquals(cache.getCacheSetSize(1), 1);
    }

    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void multipleSetPutsDels() {
        KVCache cache = new KVCache(2, 4);
        cache.put("1", "hi");
        cache.put("2", "there");
        assertEquals("hi", cache.get("1"));
        assertEquals("there", cache.get("2"));
        assertEquals(cache.getCacheSetSize(0), 1);
        assertEquals(cache.getCacheSetSize(1), 1);
        cache.del("1");
        assertEquals(cache.getCacheSetSize(0), 1);
        assertEquals(cache.getCacheSetSize(1), 0);
        cache.del("1");
        assertEquals(cache.getCacheSetSize(0), 1);
        assertEquals(cache.getCacheSetSize(1), 0);
        cache.del("2");
        assertEquals(cache.getCacheSetSize(0), 0);
        assertEquals(cache.getCacheSetSize(1), 0);
    }

    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void multipleSetsSamePuts() {
        KVCache cache = new KVCache(3, 15);
        cache.put("1", "hi");
        //System.out.println(cache.toXML());
        assertEquals(cache.getCacheSetSize(0), 0);
        assertEquals(cache.getCacheSetSize(1), 1);
        assertEquals(cache.getCacheSetSize(2), 0);
        assertEquals("hi", cache.get("1"));
        cache.put("1", "there");
        //System.out.println(cache.toXML());
        assertEquals("there", cache.get("1"));
        assertEquals(cache.getCacheSetSize(0), 0);
        assertEquals(cache.getCacheSetSize(1), 1);
        assertEquals(cache.getCacheSetSize(2), 0);
    }

    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void multipleSetsTooManyPuts() {
        KVCache cache = new KVCache(2, 3);
        cache.put("2", "hi");
        cache.put("4", "there");
        cache.put("6", "testing");
        assertEquals(3, cache.getCacheSetSize(0));
        assertEquals(0, cache.getCacheSetSize(1));
        assertEquals("hi", cache.get("2"));
        assertEquals("there", cache.get("4"));
        assertEquals("testing", cache.get("6"));
        cache.put("8", "new");
        assertEquals("new", cache.get("8"));
        assertEquals("there", cache.get("4"));
        assertEquals("testing", cache.get("6"));
        assertEquals(null, cache.get("2"));
        assertEquals(3, cache.getCacheSetSize(0));
        assertEquals(0, cache.getCacheSetSize(1));
        cache.put("1", "hi");
        cache.put("3", "there");
        cache.put("5", "testing");
        assertEquals(3, cache.getCacheSetSize(0));
        assertEquals(3, cache.getCacheSetSize(1));
        //System.out.println(cache.toXML());
        assertEquals("hi", cache.get("1"));
        assertEquals("there", cache.get("3"));
        assertEquals("testing", cache.get("5"));
        //System.out.println(cache.toXML());
    }

    @Test(timeout = kTimeoutQuick)
    @Category(AG_PROJ3_CODE.class)
    @AGTestDetails(points = 1, desc = "")
    public void multipleSetsTooManyPutsWithGets() {
        KVCache cache = new KVCache(2, 3);
        cache.put("2", "hi");
        cache.put("4", "there");
        String tooLong = "testingoutareallylongstringandseeinghowlongitshashcode"
                + "isihatetestingijustwanttosleepalldaylalalallaalalalalalla"
                + "lalalalallalallalalalalalalalallalairisisawesomelalalalala";
        cache.put(tooLong, "THIS BETTER WORK");
        cache.put("woah", "a");
        cache.put("wau", "b");
        cache.put("filling", "c");
        assertEquals(cache.get("2"), "hi");
        cache.put("6", "new");
        assertEquals(cache.get("4"), null);
        assertEquals(cache.get(tooLong), "THIS BETTER WORK");
        assertEquals(cache.get("woah"), "a");
        assertEquals(cache.get("wau"), "b");
        assertEquals(cache.get("filling"), "c");
        cache.put("iris", "rocks!");
        assertEquals(cache.get("woah"), null);
        System.out.println(cache.toXML());
    }
    
    @Test(timeout = kTimeoutQuick)
    //@Category(AG_PROJ3_CODE.class)
    //@AGTestDetails(points = 1, desc = "")
    public void testSecondChanceAlgo() {
        KVCache cache = new KVCache(1, 4);
        cache.put("B", "hi");
        cache.put("A", "there");
        cache.get("A");
        cache.put("D", "don't care");
        cache.put("C", "also don't care");
        String[] expectedKeys = {"B","A","D","C"};
        assertEquals(cache.getFirstSetKeys(), expectedKeys);
        boolean[] expectedRefs = {false,true,false,false};
        //System.out.println("expected: " + expectedRefs[0] + " " + expectedRefs[1] + " " + expectedRefs[2] + " " + expectedRefs[3]);
        //System.out.println("actual: " + cache.getFirstSetRefs()[0] + " " + cache.getFirstSetRefs()[1] + " " + cache.getFirstSetRefs()[2] + " " + cache.getFirstSetRefs()[3]);
        assertEquals(cache.getFirstSetRefs()[0], expectedRefs[0]);
        assertEquals(cache.getFirstSetRefs()[1], expectedRefs[1]);
        assertEquals(cache.getFirstSetRefs()[2], expectedRefs[2]);
        assertEquals(cache.getFirstSetRefs()[3], expectedRefs[3]);
        
        cache.put("F", "F arrives");
        String[] expectedKeys2 = {"A","D","C","F"};
        assertEquals(cache.getFirstSetKeys(), expectedKeys2);
        boolean[] expectedRefs2 = {true,false,false,false};
        assertEquals(cache.getFirstSetRefs()[0], expectedRefs2[0]);
        assertEquals(cache.getFirstSetRefs()[1], expectedRefs2[1]);
        assertEquals(cache.getFirstSetRefs()[2], expectedRefs2[2]);
        assertEquals(cache.getFirstSetRefs()[3], expectedRefs2[3]);
        
        cache.get("D");
        cache.put("E", "E arrives");
        String[] expectedKeys3 = {"F","A","D","E"};
        assertEquals(cache.getFirstSetKeys(), expectedKeys3);
        
        boolean[] expectedRefs3 = {false,false,false,false};
        assertEquals(cache.getFirstSetRefs()[0], expectedRefs3[0]);
        assertEquals(cache.getFirstSetRefs()[1], expectedRefs3[1]);
        assertEquals(cache.getFirstSetRefs()[2], expectedRefs3[2]);
        assertEquals(cache.getFirstSetRefs()[3], expectedRefs3[3]);
    }
    
    @Test(timeout = kTimeoutQuick)
    //@Category(AG_PROJ3_CODE.class)
    //@AGTestDetails(points = 1, desc = "")
    public void verfiyToXmlOfEmptyCache() {
        KVCache cache = new KVCache(1, 4);
        System.out.println(cache.toXML());
    }
}



























