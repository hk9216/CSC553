package simpledb;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */

    PageId _pid;
    int _tupleno;
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        this._pid=pid;
        this._tupleno=tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return _tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return _pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
       
    try {

      if (o == this) {
            return true;
        }
       else if (!(o instanceof RecordId)) {
            return false;
        }
      else {
        return ((RecordId) o).getPageId().equals(getPageId())
                && ((RecordId) o).tupleno() == tupleno();
    }}
       catch (Exception ex) {
           throw new UnsupportedOperationException("Issue in RecordId equal method");
       }
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        try {
       int dt [] = new int[2];
        dt[0] = _pid.hashCode();
        dt[1] = _tupleno;
        return Arrays.hashCode(dt);
      }
     catch( Exception ex){
      throw new UnsupportedOperationException("Error in hashcode method of RecordID");
     }
       
    }

}
