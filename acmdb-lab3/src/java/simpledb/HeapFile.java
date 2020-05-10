package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

    private File file;
    private TupleDesc tupleDesc;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
       return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
       Page page = null;
       int pageSize = BufferPool.getPageSize();
       byte[] data = new byte[pageSize];
       int offset = pid.pageNumber() * pageSize;
       try {
           RandomAccessFile in = new RandomAccessFile(this.file, "r");
           in.seek(offset);
           in.read(data, 0, pageSize);
           page = new HeapPage((HeapPageId)(pid), data);
       }catch (Exception e){
           e.printStackTrace();
           System.exit(1);
       }
       return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageSize = BufferPool.getPageSize();
        PageId pid = page.getId();
        int offset = pid.pageNumber() * pageSize;
        try {
            RandomAccessFile in = new RandomAccessFile(this.file, "rw");
            in.seek(offset);
            in.write(page.getPageData());
        }catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        return (int) file.length() / pageSize;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int np = numPages();
        t.setRecordId(null);
        HeapPage heapPage;
        int tableid = getId();
        ArrayList<Page> dirtyPage = new ArrayList<>();
        for(int i = 0; i < np; ++i){
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableid, i), Permissions.READ_ONLY);
            if(heapPage.getNumEmptySlots() > 0){
                heapPage.insertTuple(t);
                dirtyPage.add(heapPage);
                return dirtyPage;
            }
        }
        heapPage = new HeapPage(new HeapPageId(getId(), np), HeapPage.createEmptyPageData());
        heapPage.insertTuple(t);
        writePage(heapPage);
        dirtyPage.add(heapPage);
        return dirtyPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = t.getRecordId().getPageId();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        ArrayList<Page> dirtyPage = new ArrayList<>();
        dirtyPage.add(heapPage);
        try {
            writePage(heapPage);
        }catch (IOException e){
            System.out.println(e.toString());
        }
        return dirtyPage;
    }


    class HeapFileIterator implements DbFileIterator{

        private boolean isOpen;
        private int currentPageNumber;
        private Iterator<Tuple> currentPageIterator;
        private TransactionId transactionId;

        public HeapFileIterator(TransactionId tid){
            this.isOpen = false;
            this.currentPageNumber = 0;
            this.currentPageIterator = null;
            this.transactionId = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            HeapPageId pageId = new HeapPageId(getId(), this.currentPageNumber);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.transactionId, pageId,
                    Permissions.READ_ONLY);
            currentPageIterator = page.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(!isOpen || numPages() == 0)
                return false;
            else{
                if(currentPageIterator.hasNext()) return true;
                else{
                    int np = numPages();
                    while (!currentPageIterator.hasNext() && currentPageNumber < np - 1){
                        currentPageNumber += 1;
                        HeapPageId pageId = new HeapPageId(getId(), this.currentPageNumber);
                        HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.transactionId, pageId,
                                Permissions.READ_ONLY);
                        currentPageIterator = page.iterator();
                    }
                    return currentPageIterator.hasNext();
                }
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!hasNext())
                throw new NoSuchElementException("No remaining element.");
            else
                if(currentPageIterator.hasNext())
                    return currentPageIterator.next();
                else {
                    currentPageNumber += 1;
                    HeapPage newPage = (HeapPage)(Database.getBufferPool().getPage(transactionId,
                            new HeapPageId(getId(), currentPageNumber), Permissions.READ_ONLY));
                    currentPageIterator = newPage.iterator();
                    return currentPageIterator.next();
                }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.currentPageNumber = 0;
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(this.transactionId,
                    new HeapPageId(getId(),currentPageNumber),
                    Permissions.READ_ONLY);
            this.currentPageIterator = p.iterator();
        }

        @Override
        public void close() {
            isOpen = false;
            currentPageNumber = 0;
            currentPageIterator = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

