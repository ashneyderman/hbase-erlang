package me.cmoz.diver;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangPid;
import com.ericsson.otp.erlang.OtpErlangRef;
import com.ericsson.otp.erlang.OtpErlangTuple;
import com.ericsson.otp.erlang.OtpMbox;
import com.stumbleupon.async.Callback;
import org.hbase.async.ColumnPrefixFilter;
import org.hbase.async.ColumnRangeFilter;
import org.hbase.async.CompareFilter;
import org.hbase.async.FilterList;
import org.hbase.async.FirstKeyOnlyFilter;
import org.hbase.async.FuzzyRowFilter;
import org.hbase.async.KeyOnlyFilter;
import org.hbase.async.KeyRegexpFilter;
import org.hbase.async.KeyValue;
import org.hbase.async.RegexStringComparator;
import org.hbase.async.ScanFilter;
import org.hbase.async.Scanner;
import org.hbase.async.ValueFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class AsyncScanner implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
  private static final OtpErlangAtom ROW_ATOM = new OtpErlangAtom("row");
  private static final OtpErlangAtom DONE_ATOM = new OtpErlangAtom("done");

  private final OtpErlangTuple from;
  private final OtpMbox mbox;
  private final OtpErlangRef ref;
  private final Scanner scanner;
  int numRows = Integer.MAX_VALUE;

  public AsyncScanner(OtpErlangTuple from, OtpMbox mbox, OtpErlangRef ref, Scanner scanner, OtpErlangList options)
        throws OtpErlangDecodeException {
    this.from = from;
    this.mbox = mbox;
    this.ref = ref;
    this.scanner = scanner;

    // prevent returning partial row by default
    scanner.setMaxNumKeyValues(-1);

    for (final OtpErlangObject option : options) {
      final OtpErlangTuple tuple = (OtpErlangTuple) option;
      final OtpErlangObject[] tupleElements = tuple.elements();
      final String optionName = ((OtpErlangAtom) tupleElements[0]).atomValue();
      final OtpErlangObject optionValue = tupleElements[1];

      switch(optionName) {
      case "num_rows":
        numRows = (int)((OtpErlangLong) optionValue).longValue();
        scanner.setMaxNumRows(numRows);
        break;

      case "family":
        if(optionValue.getClass().isAssignableFrom(OtpErlangList.class)) {
          scanner.setFamilies(familiesFromList((OtpErlangList) optionValue));
        } else {
          scanner.setFamily(((OtpErlangBinary) optionValue).binaryValue());
        }
        break;
      case "key_regexp":
        scanner.setKeyRegexp(new String(((OtpErlangBinary) optionValue).binaryValue()));
        break;
      // TODO: setKeyRegexp(regesp, charset)
      case "max_num_bytes":
        scanner.setMaxTimestamp(((OtpErlangLong) optionValue).longValue());
        break;
      case "max_num_keyvalues":
        scanner.setMaxNumKeyValues((int)((OtpErlangLong) optionValue).longValue());
        break;
      case "max_num_rows":
        scanner.setMaxNumRows((int)((OtpErlangLong) optionValue).longValue());
        break;
      case "max_timestamp":
        scanner.setMaxTimestamp(((OtpErlangLong) optionValue).longValue());
        break;
      case "max_versions":
        scanner.setMaxVersions((int)((OtpErlangLong) optionValue).longValue());
        break;
      case "qualifier":
        scanner.setQualifier(((OtpErlangBinary) optionValue).binaryValue());
        break;
      // TODO: setQualifiers
      case "server_block_cache":
        scanner.setServerBlockCache(((OtpErlangLong) optionValue).longValue() != 0);
        break;
      case "start_key":
        scanner.setStartKey(((OtpErlangBinary) optionValue).binaryValue());
        break;
      case "stop_key":
        scanner.setStopKey(((OtpErlangBinary) optionValue).binaryValue());
        break;
      case "time_range":
        final OtpErlangObject[] timeRangeElems = ((OtpErlangTuple)optionValue).elements();
        scanner.setTimeRange(
          ((OtpErlangLong) timeRangeElems[0]).longValue(),
          ((OtpErlangLong) timeRangeElems[1]).longValue());
        break;
      case "filter":
        if(optionValue.getClass().isAssignableFrom(OtpErlangList.class)) {
          List<ScanFilter> filters = filterFromList((OtpErlangList)optionValue);
          scanner.setFilter(new FilterList(filters));
        } else {
          scanner.setFilter(filterFromTuple((OtpErlangTuple)optionValue));
        }
        break;
      default:
        final String message = String.format("Invalid scan option: \"%s\"", tuple);
        throw new OtpErlangDecodeException(message);
      }
    }
  }

  private String[] familiesFromList(OtpErlangList list) {
    String [] families = new String[list.arity()];

    for (int i = 0; i < list.arity(); i++) {
      families[i] = new String(((OtpErlangBinary) list.elementAt(i)).binaryValue());
    }

    return families;
  }

  private List<ScanFilter> filterFromList(OtpErlangList filters) {
    List<ScanFilter> result = new ArrayList<ScanFilter>();
    while (filters.iterator().hasNext()) {
      result.add(filterFromTuple((OtpErlangTuple) filters.iterator().next()));
    }
    return result;
  }

  private ScanFilter filterFromTuple(OtpErlangTuple tuple) {
    OtpErlangObject[] objs = tuple.elements();
    OtpErlangAtom name = (OtpErlangAtom)objs[0];

    switch(name.atomValue()) {
      //TODO: column pagination
      case "column_prefix":
        return new ColumnPrefixFilter(
                ((OtpErlangBinary)objs[1]).binaryValue()
        );
      case "column_range":
        return new ColumnRangeFilter(
                ((OtpErlangBinary)objs[1]).binaryValue(),
                ((OtpErlangBinary)objs[2]).binaryValue()
        );
      //TODO: compare
      case "first_key_only":
        return new FirstKeyOnlyFilter();
      case "fuzzy_row":
        return fuzzyRowFilterFromList((OtpErlangList)objs[1]);
      case "key_only":
        return new KeyOnlyFilter();
      case "key_regexp":
        return new KeyRegexpFilter(((OtpErlangBinary)objs[1]).binaryValue());
      //TODO: timestamps
      case "value":
        return new ValueFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(new String(((OtpErlangBinary)objs[1]).binaryValue())));

      default:
        throw new IllegalArgumentException("unknown filter key: " + name.atomValue());
    }
  }

  private FuzzyRowFilter fuzzyRowFilterFromList(OtpErlangList list) {
    OtpErlangObject[] objects = list.elements();
    FuzzyRowFilter.FuzzyFilterPair[] pairs = new FuzzyRowFilter.FuzzyFilterPair[objects.length];
    for(int i = 0; i < objects.length; i++) {
      OtpErlangTuple tup = (OtpErlangTuple)objects[i];
      if(tup.arity() != 2) {
        throw new IllegalArgumentException("invalid option for fuzzy row filter: " + tup.toString());
      }
      pairs[i] = new FuzzyRowFilter.FuzzyFilterPair(
              ((OtpErlangBinary)tup.elementAt(0)).binaryValue(),
              ((OtpErlangBinary)tup.elementAt(1)).binaryValue());
    }
    return new FuzzyRowFilter(Arrays.asList(pairs));
  }

  public void start() {
    scanner.nextRows()
      .addCallback(this)
      .addErrback(new ScannerErrback(from, mbox, ref));
  }

  @Override
  public Object call(ArrayList<ArrayList<KeyValue>> rows) throws Exception {
    if (rows == null) {
      sendDone();
      return null;
    }

    for(final ArrayList<KeyValue> row : rows) {
      sendRow(row);

      numRows -= 1;
      if(numRows == 0) {
        sendDone();
        return null;
      }
    }

    scanner.nextRows()
      .addCallback(this)
      .addErrback(new ScannerErrback(from, mbox, ref));
    return null;
  }

  public void sendDone() {
    final OtpErlangObject[] body = new OtpErlangObject[] {
        ref,
        DONE_ATOM,
    };

    mbox.send((OtpErlangPid) from.elementAt(0), new OtpErlangTuple(body));
  }

  public void sendRow(final ArrayList<KeyValue> data) throws Exception {
    final OtpErlangObject[] items = new OtpErlangObject[data.size()];
    int i = 0;
    for (final KeyValue keyValue : data) {
      final OtpErlangObject[] erldata = new OtpErlangObject[] {
          new OtpErlangBinary(keyValue.key()),
          new OtpErlangBinary(keyValue.family()),
          new OtpErlangBinary(keyValue.qualifier()),
          new OtpErlangBinary(keyValue.value()),
          new OtpErlangLong(keyValue.timestamp())
      };
      items[i] = new OtpErlangTuple(erldata);
      i++;
    }

    final OtpErlangObject[] body = new OtpErlangObject[] {
        ref,
        ROW_ATOM,
        new OtpErlangList(items)
    };

    mbox.send((OtpErlangPid) from.elementAt(0), new OtpErlangTuple(body));
  }
}
