package edu.uchciago.mpcs53013.streamCrime;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class CrimeRecord {
	private String primary_type;
	
	private String community_area;
	
	private String year;
	
	private String month;
	
	DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    
	public CrimeRecord(String primary_type, String community_area, String date) {
		super();
		
		
		this.setCommunity_area(community_area);
		this.setPrimary_type(primary_type);
		
		Calendar mydate = new GregorianCalendar();
		
		try {
			Date thedate = df.parse(date);
			mydate.setTime(thedate);
			
			int intmonth = mydate.get(Calendar.MONTH)+1;
			this.month = "" + (intmonth<10?("0"+intmonth):(intmonth));
			this.year = "" + mydate.get(Calendar.YEAR);
			
			System.out.println("area  -> "+this.community_area);
			System.out.println("type  -> "+this.primary_type);
			System.out.println("month  -> "+this.month);
			System.out.println("year  -> "+this.year);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public String getPrimary_type() {
		return primary_type;
	}

	public void setPrimary_type(String primary_type) {
		this.primary_type = primary_type;
	}

	public String getCommunity_area() {
		return community_area;
	}

	public void setCommunity_area(String community_area) {
		this.community_area = community_area;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}
	
	
}
