package com.wavefront.rest.api.client.manage.response;

import java.util.List;
import java.util.Map;

public class TaggedSourceBundle {

	private List<TaggedSource> sources;
	private Map<String, Long> counts;
	private Boolean hasMore;
 	
	/**
	 * 
	 */
	public TaggedSourceBundle() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the sources
	 */
	public List<TaggedSource> getSources() {
		return sources;
	}

	/**
	 * @param sources the sources to set
	 */
	public void setSources(List<TaggedSource> sources) {
		this.sources = sources;
	}

	/**
	 * @return the counts
	 */
	public Map<String, Long> getCounts() {
		return counts;
	}

	/**
	 * @param counts the counts to set
	 */
	public void setCounts(Map<String, Long> counts) {
		this.counts = counts;
	}

	/**
	 * @return the hasMore
	 */
	public Boolean getHasMore() {
		return hasMore;
	}

	/**
	 * @param hasMore the hasMore to set
	 */
	public void setHasMore(Boolean hasMore) {
		this.hasMore = hasMore;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "TaggedSourceBundle [sources=" + sources + ", counts=" + counts + ", hasMore=" + hasMore + "]";
	}

	
}
