package edu.uchciago.mpcs53013.streamCrime;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
class CrimeResponse {
	
	
	
	@JsonProperty("primary_type")
	private String primary_type;
	
	@JsonProperty("community_area")
	private String community_area;
	
	@JsonProperty("date")
	private String date;

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

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
	
	@Override
	public String toString() {
		return "primary_type: " + primary_type + " date:" + date + " community_area:" + community_area;  
	}
	
}
