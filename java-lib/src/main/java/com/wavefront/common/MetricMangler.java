package com.wavefront.common;

import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

/**
 * Handles updating the metric and source names by extracting components from the metric name.
 * There are several options considered:
 * <ul>
 *  <li>source name:</li>
 *  <ul>
 *    <li>extracted from one or more components of the metric name
 *       (where each component is separated by a '.')</li>
 *    <li>allow characters to be optionally replaced in the components extracted
 *        as source name with '.'</li>
 *  </ul>
 *  <li>metric name:</li>
 *  <ul>
 *    <li>remove components (in addition to the source name)
 *        (this allows things like 'hosts.sjc1234.cpu.loadavg.1m' to get
 *        changed to cpu.loadavg.1m after extracting sjc1234)</li>
 *  </ul>
 * </ul>
 * This code was originally mostly contained in GraphiteFormatter class and moved into a single
 * re-usable class.
 * @author Mike McLaughlin (mike@wavefront.com)
 */
public class MetricMangler {

  // Fields to extract and assemble, in order, as the host name
  private final List<Integer> hostIndices = new ArrayList<>();
  private int maxField = 0;

  // Lookup set for which indices are hostname related
  private final Set<Integer> hostIndexSet = new HashSet<>();

  // Characters which should be interpreted as dots
  @Nullable
  private final String delimiters;

  // Fields to remove
  private final Set<Integer> removeIndexSet = new HashSet<>();

  /**
   * Constructor.
   *
   * @param sourceFields comma separated field index(es) (1-based) where the source name will be
   *                     extracted
   * @param delimiters   characters to be interpreted as dots
   * @param removeFields comma separated field index(es) (1-based) of fields to remove from the
   *                     metric name
   * @throws IllegalArgumentException when one of the field index is <= 0
   */
  public MetricMangler(@Nullable String sourceFields,
                       @Nullable String delimiters,
                       @Nullable String removeFields) {
    if (sourceFields != null) {
      // Store ordered field indices and lookup set
      Iterable<String> fields = Splitter.on(",").omitEmptyStrings().trimResults().split(sourceFields);
      for (String field : fields) {
        if (field.trim().length() > 0) {
          int fieldIndex = Integer.parseInt(field);
          if (fieldIndex <= 0) {
            throw new IllegalArgumentException("Can't define a field of index 0 or less; indices must be 1-based");
          }
          hostIndices.add(fieldIndex - 1);
          hostIndexSet.add(fieldIndex - 1);
          if (fieldIndex > maxField) {
            maxField = fieldIndex;
          }
        }
      }
    }

    if (removeFields != null) {
      Iterable<String> fields = Splitter.on(",").omitEmptyStrings().trimResults().split(removeFields);
      for (String field : fields) {
        if (field.trim().length() > 0) {
          int fieldIndex = Integer.parseInt(field);
          if (fieldIndex <= 0) {
            throw new IllegalArgumentException("Can't define a field to remove of index 0 or less; indices must be 1-based");
          }
          removeIndexSet.add(fieldIndex - 1);
        }
      }
    }

    // Store as-is; going to loop through chars anyway
    this.delimiters = delimiters;
  }

  /**
   * Simple struct to store and return both the source and the updated metric.
   *
   * @see {@link #extractComponents(String)}
   */
  public static class MetricComponents {
    @Nullable
    public String source;
    @Nullable
    public String metric;
  }

  /**
   * Extracts the source from the metric name and returns the new metric name and the source name.
   *
   * @param metric the metric name
   * @return the updated metric name and the extracted source
   * @throws IllegalArgumentException when the number of segments (split on '.') is less than the
   *                                  maximum source component index
   */
  public MetricComponents extractComponents(final String metric) {
    final String[] segments = metric.split("\\.");
    final MetricComponents rtn = new MetricComponents();

    // Is the metric name long enough?
    if (segments.length < maxField) {
      throw new IllegalArgumentException(
          String.format("Metric data |%s| provided was incompatible with format.", metric));
    }

    // Assemble the newly shorn metric name, in original order
    StringBuilder buf = new StringBuilder();
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
      buf = new StringBuilder();
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
