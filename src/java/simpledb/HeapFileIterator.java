package simpledb;

import java.util.*;
import java.io.*;

public class HeapFileIterator extends AbstractDbFileIterator {

  private HeapFile _hf;
  private TransactionId _tid;

  private boolean open = false;
  private int curr_pgNo;
  private HeapPage curr_pg;
  private Iterator<Tuple> pg_iter; 

  public HeapFileIterator(TransactionId tid, HeapFile hf) {
    _hf = hf;
    _tid = tid;
  }

  /**
   * Opens the iterator
   * @throws DbException when there are problems opening/accessing the database.
   */
  public void open() throws DbException, TransactionAbortedException {
    curr_pgNo = 0;
    curr_pg = (HeapPage) Database.getBufferPool().getPage(_tid, new HeapPageId(_hf.getId(), curr_pgNo), null);
    pg_iter = curr_pg.iterator();
    open = true;
  }

  /**
   * Resets the iterator to the start.
   * @throws DbException When rewind is unsupported.
   */
  public void rewind() throws DbException, TransactionAbortedException {
    curr_pgNo = 0;
    curr_pg = (HeapPage) Database.getBufferPool().getPage(_tid, new HeapPageId(_hf.getId(), curr_pgNo), null);
    pg_iter = curr_pg.iterator();
  }

  /**
   * Closes the iterator.
   */
  public void close() {
    open = false;
    super.close();
  }

  /** Reads the next tuple from the underlying source.
  @return the next Tuple in the iterator, null if the iteration is finished. */
  protected Tuple readNext() throws DbException, TransactionAbortedException {
    if (!open) {
      return null;
    }
    if (!pg_iter.hasNext()) {
      return null;
    }
    Tuple next = pg_iter.next();
    if (next != null) {
      return next;
    }
    else {
      curr_pgNo++;
      curr_pg = (HeapPage) Database.getBufferPool().getPage(_tid, new HeapPageId(_hf.getId(), curr_pgNo), null);
      if (curr_pg == null) {
        return null;
      }
      else {
        pg_iter = curr_pg.iterator();
        return pg_iter.next();
      }
    }
  }
  
}
