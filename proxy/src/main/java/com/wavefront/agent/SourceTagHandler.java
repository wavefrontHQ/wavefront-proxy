package com.wavefront.agent;

import java.util.List;

import io.netty.channel.ChannelHandler;
import wavefront.report.ReportSourceTag;

/**
 * Interface for a handler of Source Tags.
 *
 * @author Suranjan Pramanik (suranjan@wavefront.com).
 */
public interface SourceTagHandler {

  void reportSourceTags(List<ReportSourceTag> sourceTags);

  void processSourceTag(final String msg);

}
