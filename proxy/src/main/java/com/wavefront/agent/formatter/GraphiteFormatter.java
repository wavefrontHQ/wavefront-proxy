package com.wavefront.agent.formatter;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Specific formatter for the graphite/collectd world of metric-munged names.
 * <p/>
 * Created by dev@wavefront.com on 11/5/14.
 */
public class GraphiteFormatter extends Formatter {

  // Fields to extract and assemble, in order, as the host name
  private final List<Integer> hostIndices = new ArrayList<Integer>();
  private int maxField = 0;

  // Lookup set for which indices are hostname related
  private Set<Integer> hostIndexSet = new HashSet<Integer>();

  // Characters which should be interpreted as dots
  private final String delimiters;

  private final AtomicLong ops = new AtomicLong();

  public GraphiteFormatter(String strFields, String strDelimiters) {
    Preconditions.checkNotNull(strFields, "strFields must be defined");
    Preconditions.checkNotNull(strDelimiters, "strFields must be defined");
    // Store ordered field indices and lookup set
    for (String field : strFields.split(",")) {
      if (field.trim().length() > 0) {
        int iField = Integer.parseInt(field);
        if (iField <= 0) {
          throw new IllegalArgumentException("Can't define a field of index 0 or less; indices must be 1-based");
        }
        hostIndices.add(iField - 1);
        hostIndexSet.add(iField - 1);
        if (iField > maxField) {
          maxField = iField;
        }
      }
    }
    // Store as-is; going to loop through chars anyway
    delimiters = strDelimiters;
  }

  public long getOps() {
    return ops.get();
  }

  public String format(String mesg) throws IllegalArgumentException {
    // Resting place for output
    StringBuffer finalMesg = new StringBuffer();

    // 1. Extract fields
    String[] regions = mesg.trim().split(" ");
    String[] segments = regions[0].split("\\.");

    // Is the metric name long enough?
    if (segments.length < maxField) {
      throw new IllegalArgumentException("Metric data provided was incompatible with format.");
    }

    // 2. Assemble the newly shorn metric name, in original order
    for (int i = 0; i < segments.length; i++) {
      if (!hostIndexSet.contains(i)) {
        if (finalMesg.length() > 0) {
          finalMesg.append('.');
        }
        finalMesg.append(segments[i]);
      }
    }
    finalMesg.append(" ");

    // 3. Add back value, timestamp if existent, etc. We don't assume you have point tags
    //  because your first tag BETTER BE A HOST, BUDDY.
    for (int index = 1; index < regions.length; index++) {
      finalMesg.append(regions[index]);
      finalMesg.append(" ");
    }

    // 4. Loop over host components in configured order, and replace all delimiters with dots
    finalMesg.append("source=");
    for (int f = 0; f < hostIndices.size(); f++) {
      char[] segmentChars = segments[hostIndices.get(f)].toCharArray();
      for (int i = 0; i < segmentChars.length; i++) {
        for (int c = 0; c < delimiters.length(); c++) {
          if (segmentChars[i] == delimiters.charAt(c)) {
            segmentChars[i] = '.'; // overwrite it
          }
        }
      }
      if (f > 0) {
        // join host segments with dot, if you're after the first one
        finalMesg.append('.');
      }
      finalMesg.append(segmentChars);
    }

    ops.incrementAndGet();

    return finalMesg.toString();
  }
}
