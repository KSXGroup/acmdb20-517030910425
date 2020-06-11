package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 400;


    private static class Node{
        private Page page;
        private Node prev;
        private Node next;

        public Node(Page page, Node prev, Node next){
            this.page = page;
            this.prev = prev;
            this.next = next;
        }

        public void setPage(Page page){
            this.page = page;
        }
        public Page getPage(){return this.page;}
    }

    private static class LRUCache{
        private Node head;
        private Node tail;
        private int numPages;
        private int maxPages;
        private HashMap<PageId, Node> pageMap;

        public LRUCache(int maxPages){
            this.head = null;
            this.tail = null;
            this.numPages = 0;
            this.maxPages = maxPages;
            this.pageMap = new HashMap<PageId, Node>();
        }

        private Node removeNodeFromList(Node n){
            if(n == this.head)
                this.head = n.next;
            if(n == this.tail)
                this.tail = n.prev;
            if(n.prev != null)
                n.prev.next = n.next;
            if(n.next != null)
                n.next.prev = n.prev;
            n.prev = null;
            n.next = null;
            return n;
        }

        private void insertAtBeginning(Node n){
            if(this.head == null) {
                assert this.tail == null;
                this.head = n;
                this.tail = n;
                n.prev = null;
                n.next = null;
            }else{
                this.head.prev = n;
                n.next = head;
                this.head = n;
                n.prev = null;
            }
        }

        private Node removeTail(){
            Node tail = this.tail;
            Node newtail = tail.prev;
            if(tail.prev == null){
                assert this.head == this.tail;
                this.head = null;
            }
            tail.prev = null;
            tail.next = null;
            this.tail = newtail;
            return tail;
        }

        public synchronized Page get(PageId pageId){
            if(!pageMap.containsKey(pageId))
                return null;
            else{
                Node n = pageMap.get(pageId);
                Node n1 = removeNodeFromList(n);
                insertAtBeginning(n1);
                return n.page;
            }
        }

        public synchronized void put(PageId pageId, Page page) throws IOException{
            if(this.pageMap.containsKey(pageId)){
                Node n = pageMap.get(pageId);
                Node n1 = removeNodeFromList(n);
                insertAtBeginning(n1);
            }else {
                Node n;
                if (this.numPages == this.maxPages) {
                    n = removeTail();
                    Database.getCatalog().getDatabaseFile(n.getPage().getId().getTableId()).writePage(n.getPage());
                    this.pageMap.remove(n.page.getId());
                    n.setPage(page);
                }
                else {
                    n = new Node(page, null, null);
                    this.numPages += 1;
                }
                insertAtBeginning(n);
                this.pageMap.put(pageId, n);
            }
        }

        public synchronized boolean contains(PageId pageId){
            return this.pageMap.containsKey(pageId);
        }
        public synchronized Page remove(PageId pageId){
            if(this.pageMap.containsKey(pageId)){
                Node n = this.pageMap.get(pageId);
                removeNodeFromList(n);
                this.pageMap.remove(pageId);
                this.numPages -= 1;
                return n.page;
            }
            else return null;
        }
        public synchronized void clear(){
            this.head = null;
            this.tail = null;
            this.numPages = 0;
            this.pageMap.clear();
        }

        public synchronized void flushAll() throws IOException{
            for(Node n : this.pageMap.values())
                Database.getCatalog().getDatabaseFile(n.getPage().getId().getTableId()).writePage(n.getPage());
        }
    }

    LRUCache lruCache;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.lruCache = new LRUCache(numPages);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        Page page;
        if(this.lruCache.contains(pid)){
            //System.out.println("Buffer Pool hit " + pid.toString());
            page = lruCache.get(pid);
//            if (perm == Permissions.READ_WRITE)
//                page.markDirty(true, tid);
            return page;
        }
        else{
            //System.out.println("Buffer Pool not hit " + pid.toString());
            int tableId = pid.getTableId();
            page = Database.getCatalog().getDatabaseFile(tableId).readPage(pid);
            try {
                lruCache.put(pid, page);
            }catch (IOException e){
                throw new DbException("fail to put to cache");
            }
//            if (perm == Permissions.READ_WRITE)
//                page.markDirty(true, tid);
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = f.insertTuple(tid, t);
        for(Page p : dirtyPages) {
            p.markDirty(true, tid);
            lruCache.put(p.getId(), p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = t.getRecordId().getPageId();
        int tableId = pageId.getTableId();
        DbFile table = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> dirtyPages = table.deleteTuple(tid, t);
        for(Page p : dirtyPages) {
            p.markDirty(true, tid);
            lruCache.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        this.lruCache.flushAll();
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.lruCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = this.lruCache.get(pid);
        if(p != null && p.isDirty() != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
            p.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    }

}
