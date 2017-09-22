package com.sinobbd.tercelsrear.urlrank;

import java.util.Date;

import org.joda.time.DateTime;

public class DateISO extends Date {

	public DateISO(long date) {
		super(date);
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String toString() {
		Long time=super.getTime();
		DateTime tm= new DateTime(time);
		return tm.toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		
	}

}
