package smr.hadoop;

import org.apache.hadoop.io.*;

abstract class ScrewWritableComparable implements WritableComparable {
  @Override public int compareTo(Object o) {
    return compareX(o);
  }
  
  protected abstract int compareX(Object o);
}
