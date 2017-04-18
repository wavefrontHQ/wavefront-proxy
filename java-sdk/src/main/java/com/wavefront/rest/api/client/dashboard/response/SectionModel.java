package com.wavefront.rest.api.client.dashboard.response;

import java.util.List;

public class SectionModel {

	private String name;
	private List<SectionRowModel> rows;

	public SectionModel() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the rows
	 */
	public List<SectionRowModel> getRows() {
		return rows;
	}

	/**
	 * @param rows
	 *            the rows to set
	 */
	public void setRows(List<SectionRowModel> rows) {
		this.rows = rows;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SectionModel [name=" + name + ", rows=" + rows + "]";
	}

}
