package com.wavefront.rest.api.client.manage.request;

public class ManageRequest {


	private String lastEntityId;
	private Boolean desc;
	private Integer limit;

	private String source;
	private String tag;

	private String body;

	public ManageRequest() {
		super();
		// TODO Auto-generated constructor stub
	}

	public ManageRequest(String lastEntityId, Boolean desc, Integer limit, String source, String tag, String body) {
		super();
		this.lastEntityId = lastEntityId;
		this.desc = desc;
		this.limit = limit;
		this.source = source;
		this.tag = tag;
		this.body = body;
	}

	public String getLastEntityId() {
		return lastEntityId;
	}

	public void setLastEntityId(String lastEntityId) {
		this.lastEntityId = lastEntityId;
	}

	public Boolean getDesc() {
		return desc;
	}

	public void setDesc(Boolean desc) {
		this.desc = desc;
	}

	public Integer getLimit() {
		return limit;
	}

	public void setLimit(Integer limit) {
		this.limit = limit;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getBody() {
		return body;
	}

	public void setBody(String body) {
		this.body = body;
	}

	@Override
	public String toString() {
		return "ManageRequest [lastEntityId=" + lastEntityId + ", desc=" + desc + ", limit=" + limit + ", source="
				+ source + ", tag=" + tag + ", body=" + body + "]";
	}

}
