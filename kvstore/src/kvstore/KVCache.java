package kvstore;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import kvstore.xml.KVCacheEntry;
import kvstore.xml.KVCacheType;
import kvstore.xml.KVSetType;
import kvstore.xml.ObjectFactory;

import java.util.LinkedList;
import java.util.ArrayList;


/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET).
 * If a set is full and another entry is added, an entry is dropped based on
 * the eviction policy.
 */
public class KVCache implements KeyValueInterface {

    /**
     * Constructs a second-chance-replacement cache.
     *
     * @param numSets the number of sets this cache will have
     * @param maxElemsPerSet the size of each set
     */

    private LinkedList<Container>[] sets;
    private int maxElemsPerSet;
    private int numSets;

    private ReentrantLock[] setLocks;

    @SuppressWarnings("unchecked")
    public KVCache(int numSets, int maxElemsPerSet) {
      this.numSets = numSets;
      this.sets = (LinkedList<Container>[]) new LinkedList<?>[numSets];
      this.setLocks = new ReentrantLock[numSets];

      this.maxElemsPerSet = maxElemsPerSet;

      for (int i = 0; i < sets.length; i++) {
        this.sets[i] = new LinkedList<Container>();
        this.setLocks[i] = new ReentrantLock();
      }

        // implement me
    }

    /**
     * Retrieves an entry from the cache.
     * Assumes access to the corresponding set has already been locked by the
     * caller of this method.
     *
     * @param  key the key whose associated value is to be returned.
     * @return the value associated to this key or null if no value is
     *         associated with this key in the cache
     */
    @Override
    public String get(String key) {
        // implement me
        int setVal = getSetVal(key);

        LinkedList<Container> queue = this.sets[setVal];
        String value = null;
        Container currElem;
        boolean found = false;

        for (int i = 0; i < queue.size(); i++) {
          currElem = queue.get(i);
          if (currElem.key == key) {
            value = currElem.value;
            currElem.ref = true;
            found = true;
            break;
          }
        }

        return value;
        //return null;
    }

    private int getSetVal(String key) {
      int hashCode = (key.hashCode() % this.numSets);
      if (hashCode < 0) {
        hashCode += this.numSets;
      }
      return hashCode;
    }

    /**
     * Adds an entry to this cache.
     * If an entry with the specified key already exists in the cache, it is
     * replaced by the new entry. When an entry is replaced, its reference bit
     * will be set to True. If the set is full, an entry is removed from
     * the cache based on the eviction policy. If the set is not full, the entry
     * will be inserted behind all existing entries. For this policy, we suggest
     * using a LinkedList over an array to keep track of entries in a set since
     * deleting an entry in an array will leave a gap in the array, likely not
     * at the end. More details and explanations in the spec. Assumes access to
     * the corresponding set has already been locked by the caller of this
     * method.
     *
     * @param key the key with which the specified value is to be associated
     * @param value a value to be associated with the specified key
     */
    @Override
    public void put(String key, String value) {
      int setVal = getSetVal(key);

      LinkedList<Container> queue = this.sets[setVal];

      Container elem = new Container(key, value);

      boolean existing = false;
      Container currElem;
      for (int i = 0; i < queue.size(); i++) {
        currElem = queue.get(i);

        if (currElem.key == key) {
          currElem.value = value;
          currElem.ref = true;
          existing = true;
          break;
        }
      }

      if (existing == false) {
        if (queue.size() < this.maxElemsPerSet) {
          queue.add(elem);
        } else {
          if (existing == false) {
            boolean swap = false;
            while (swap == false) {
              currElem = queue.get(0);
              if (currElem.ref == false) {
                queue.remove(0);
                queue.add(elem);
                swap = true;
                break;
              } else {
                Container moveElem = queue.remove(0);
                moveElem.ref = false;
                queue.add(moveElem);
              }
            }
            if (swap == false) {
              queue.remove(0);
              queue.add(elem);
            }
          }
        }
      }
      //use String.hashcode()
        // implement me
    }

    /**
     * Removes an entry from this cache.
     * Assumes access to the corresponding set has already been locked by the
     * caller of this method. Does nothing if called on a key not in the cache.
     *
     * @param key key with which the specified value is to be associated
     */
    @Override
    public void del(String key) {

      int setVal = getSetVal(key);

      LinkedList<Container> queue = this.sets[setVal];

      Container currElem;

      for (int i = 0; i < queue.size(); i++) {
        currElem = queue.get(i);
        if (currElem.key == key) {
          queue.remove(i);
          break;
        }
      }
        // implement me


    }

    /**
     * Get a lock for the set corresponding to a given key.
     * The lock should be used by the caller of the get/put/del methods
     * so that different sets can be #{modified|changed} in parallel.
     *
     * @param  key key to determine the lock to return
     * @return lock for the set that contains the key
     */

    public Lock getLock(String key) {

      int setVal = getSetVal(key);
      ReentrantLock setLock = this.setLocks[setVal];

    	return setLock;
    	//implement me

    }

    /**
     * Get the size of a given set in the cache.
     * @param cacheSet Which set.
     * @return Size of the cache set.
     */
    int getCacheSetSize(int cacheSet) {
        // implement me
        LinkedList<Container> queue = this.sets[cacheSet];

        return queue.size();
    }

    private void marshalTo(OutputStream os) throws JAXBException {
        JAXBContext context = JAXBContext.newInstance(KVCacheType.class);
        Marshaller marshaller = context.createMarshaller();
        marshaller.setProperty("com.sun.xml.internal.bind.xmlHeaders", "<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, false);
        marshaller.marshal(getXMLRoot(), os);
    }

    private JAXBElement<KVCacheType> getXMLRoot() throws JAXBException {
        ObjectFactory factory = new ObjectFactory();
        KVCacheType xmlCache = factory.createKVCacheType();
            // implement me
        ArrayList<KVSetType> xmlSets = (ArrayList<KVSetType>) xmlCache.getSet();
        KVSetType kvSet;
        ArrayList<KVCacheEntry> cacheEntries;
        KVCacheEntry cacheEntry;

        LinkedList<Container> currSet;
        Container currCont;
        for (int i = 0; i < this.sets.length; i++) {
          kvSet = factory.createKVSetType();
          cacheEntries = (ArrayList<KVCacheEntry>) kvSet.getCacheEntry();

          currSet = this.sets[i];

          for (int j = 0; j < currSet.size(); j++) {
            currCont = currSet.get(j);
            cacheEntry = factory.createKVCacheEntry();
            cacheEntry.setKey(currCont.key);
            cacheEntry.setValue(currCont.value);
            cacheEntry.setIsReferenced(String.valueOf(currCont.ref));
            cacheEntries.add(cacheEntry);
          }
          xmlSets.add(kvSet);
        }

        return factory.createKVCache(xmlCache);
    }

    /**
     * Serialize this store to XML. See spec for details on output format.
     */
    public String toXML() {
        // implement me
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            marshalTo(os);
        } catch (JAXBException e) {
            //e.printStackTrace();
        }
        return os.toString();
    }
    @Override
    public String toString() {
        return this.toXML();
    }
    
    //For testing only. Don't use.
    public String[] getFirstSetKeys()
    {
        LinkedList<Container> setEntryList = sets[0];
        String[] keys = new String[setEntryList.size()];
        
        for (int i = 0; i < setEntryList.size(); i++)
        {
            Container entry = setEntryList.get(i);
            keys[i] = entry.key;
        }
        return keys;
    }
    
    //For testing only. Don't use.
    public boolean[] getFirstSetRefs()
    {
        LinkedList<Container> setEntryList = sets[0];
        boolean[] refs = new boolean[setEntryList.size()];
        
        for (int i = 0; i < setEntryList.size(); i++)
        {
            Container entry = setEntryList.get(i);
            refs[i] = entry.ref;
        }
        return refs;
    }

    private static class Container {

      public final String key;
      public String value;
      public boolean ref;
      public Container(String key, String value) {
        this.key = key;
        this.value = value;
        this.ref = false;
      }
    }

}
