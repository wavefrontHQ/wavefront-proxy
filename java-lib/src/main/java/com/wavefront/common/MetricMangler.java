package com.wavefront.common;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Handles updating the metric and source names by extracting components
 * from the metric name.
 * There are several options considered:
 *   - source name:
 *     - extracted from one or more components of the metric name
 *       (where each component is separated by a '.')
 *     - allow characters to be optionally replaced in the components extracted
 *       as source name with '.'
 *   - metric name:
 *     - remove components (in addition to the source name)
 *       (this allows things like 'hosts.sjc1234.cpu.loadavg.1m' to get
 *        changed to cpu.loadavg.1m after extracting sjc1234)
 *
 */
public class MetricMangler {

  // Fields to extract and assemble, in order, as the host name
  private final List<Integer> hostIndices = new ArrayList<Integer>();
  private int maxField = 0;

  // Lookup set for which indices are hostname related
  private Set<Integer> hostIndexSet = new HashSet<Integer>();

  // Characters which should be interpreted as dots
  private final String delimiters;

  // Fields to remove
  private Set<Integer> removeIndexSet = new HashSet<Integer>();

  public MetricMangler(String strFields, String strDelimiters, String strFieldsToRemove) {
    if (strFields != null) {
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
    }

    if (strFieldsToRemove != null) {
      for (String field : strFieldsToRemove.split(",")) {
        if (field.trim().length() > 0) {
          int iField = Integer.parseInt(field);
          if (iField <= 0) {
            throw new IllegalArgumentException("Can't define a field to remove of index 0 or less; indices must be 1-based");
          }
          removeIndexSet.add(iField - 1);
        }
      }
    }

    // Store as-is; going to loop through chars anyway
    delimiters = strDelimiters;
  }

  /**
   * Simple struct to return both the source and the updated metric
   * from {@link #getSourceNameFromMetric(String)}
   */
  public static class MetricComponents {
    public String source;
    public String metric;
  }

  /**
   * Extracts the source from the metric name and returns the new metric name
   * and the source name.
   * @param metric the metric name
   * @return the updated metric name and the extracted source
   */
  public MetricComponents extractComponents(final String metric) {
    final String[] segments = metric.split("\\.");
    final MetricComponents rtn = new MetricComponents();

    // Is the metric name long enough?
    if (segments.length < maxField) {
      throw new IllegalArgumentException("Metric data provided was incompatible with format.");
    }

    // Assemble the newly shorn metric name, in original order
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < segments.length; i++) {
      final String segment = segments[i];
      if (!hostIndexSet.contains(i) && !removeIndexSet.contains(i)) {
        if (buf.length() > 0) {
          buf.append('.');
        }
        buf.append(segment);
      }
    }
    rtn.metric = buf.toString();

    // Loop over host components in configured order, and replace all delimiters with dots
    if (hostIndices != null && !hostIndices.isEmpty()) {
      buf = new StringBuffer();
      for (int f = 0; f < hostIndices.size(); f++) {
        char[] segmentChars = segments[hostIndices.get(f)].toCharArray();
        if (delimiters != null && !delimiters.isEmpty()) {
          for (int i = 0; i < segmentChars.length; i++) {
            for (int c = 0; c < delimiters.length(); c++) {
              if (segmentChars[i] == delimiters.charAt(c)) {
                segmentChars[i] = '.'; // overwrite it
              }
            }
          }
        }
        if (f > 0) {
          // join host segments with dot, if you're after the first one
          buf.append('.');
        }
        buf.append(segmentChars);
      }
      rtn.source = buf.toString();
    } else {
      rtn.source = null;
    }

    return rtn;
  }
    

}
