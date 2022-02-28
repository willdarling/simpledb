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
    
    // Implementation of DbFileIterator
    private class HeapFileIterator implements DbFileIterator {
        private HeapFile file;
        private TransactionId tId;
        
        private int curPageN;
        private Iterator<Tuple> curPageIter;
        
        public HeapFileIterator(HeapFile file, TransactionId tId) {
            this.file = file;
            this.tId = tId;
            this.curPageIter = null;
        }
        
        @Override
        public void open() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            if (curPageIter != null) {
                throw new DbException("HeapFileIterator is already open. Can't open again");
            }
            
            // Get the iterator for the first page
            this.curPageN = 0;
            HeapPageId curPId = new HeapPageId(this.file.getId(), this.curPageN);
            HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(this.tId, curPId, Permissions.READ_WRITE);
            this.curPageIter = curPage.iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            if (this.curPageIter == null) {
                return false;
            }
            
            // Check if current page still has next tuple
            if (this.curPageIter.hasNext()) {
                return this.curPageIter.hasNext();
            } else {
                // Check if there are more pages in the file
                while (this.curPageN < this.file.numPages() - 1) {
                    // Get the next page's iterator
                    this.curPageN++;
                    HeapPageId curPId = new HeapPageId(this.file.getId(), this.curPageN);
                    HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(this.tId, curPId, Permissions.READ_WRITE);
                    this.curPageIter = curPage.iterator();
                    
                    // Check if the new page has tuples on it. If not - go to the next one, if it exists.
                    if (!this.curPageIter.hasNext()) {
                        continue;
                    } else {
                        return this.curPageIter.hasNext();
                    }
                }
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // TODO Auto-generated method stub
            if (!this.hasNext())
                throw new NoSuchElementException("Tried to fetch next when there was no next");
            return this.curPageIter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            this.close();
            this.open();
        }

        @Override
        public void close() {
            // TODO Auto-generated method stub
            this.curPageIter = null;
        }  
    }
    
    private File file;
    private TupleDesc td;
    private int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td; 
        this.tableId = f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageSize = Database.getBufferPool().getPageSize();
        byte[] pageData = new byte[pageSize];
        int pageN = pid.getPageNumber();
        
        if (pageN * pageSize > file.length())
            throw new IllegalArgumentException();
        
        try {
            DataInputStream dis = new DataInputStream(new FileInputStream(this.file));
            dis.skip(pageN * pageSize);
            dis.readFully(pageData, 0, pageSize);
            HeapPageId resultPId = new HeapPageId(this.tableId, pageN);
            HeapPage resultPage = new HeapPage(resultPId, pageData);
            return resultPage;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            System.out.println("Couldn't read a page into a heapfile");
        }
        
        return null;
    }

    // see DbFile.java for javadocs
    // Different ways to write to file in Java: https://www.baeldung.com/java-write-to-file
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageSize = Database.getBufferPool().getPageSize();
        int pageN = page.getId().getPageNumber();
        
        try {
            RandomAccessFile dos = new RandomAccessFile(this.file, "rw");
            dos.seek(pageN * pageSize);
            dos.write(page.getPageData(), 0, pageSize);
            dos.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            System.out.println("Couldn't write a page into a heapfile");
        }
        
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) this.file.length() / Database.getBufferPool().getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> result = new ArrayList<Page>();
        for (int i = 0; i < this.numPages(); i++) {
            // Try putting a tuple on the page
            try {
                HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.tableId, i), Permissions.READ_WRITE);
                curPage.insertTuple(t);
                result.add(curPage);
                return result;
            } catch (DbException e) {
                continue;
            }
        }
        
        // If at this point - we couldn't add the page. Create an empty one
        HeapPageId newPId = new HeapPageId(this.tableId, this.numPages());
        HeapPage newPage = new HeapPage(newPId, HeapPage.createEmptyPageData());
        this.writePage(newPage);
        
        // After adding a new page - try inserting again
        return this.insertTuple(tid, t);    
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> result = new ArrayList<Page>();
        for (int i = 0; i < this.numPages(); i++) {
            // Try deleting a tuple from the page
            try {
                HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.tableId, i), Permissions.READ_WRITE);
                curPage.deleteTuple(t);
                result.add(curPage);
                return result;
            } catch (DbException e) {
                continue;
            }
        }
        
        throw new DbException("Couldn't delete the tuple from the HeapFile");
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

}

