package com.threads.commons;

public class Station {

    // Unique Station id for a Station
	private String stationID;
	// Total of TMAX temperatures for all records for a station
	private Double tmaxTotal;
	// Total number of records for a Station
	private int recordCount;
	// Average TMAX temperature for the station
	private Double averageTMAX;
	
	public Station(String stationID, Double tmaxTotal, int recordCount, Double averageTMAX) {
		super();
		this.stationID = stationID;
		this.tmaxTotal = tmaxTotal;
		this.recordCount = recordCount;
		this.averageTMAX = averageTMAX;
	}
	public String getStationID() {
		return stationID;
	}
	public void setStationID(String stationID) {
		this.stationID = stationID;
	}
	public Double getTmaxTotal() {
		return tmaxTotal;
	}
	public void setTmaxTotal(Double tmaxTotal) {
		this.tmaxTotal = tmaxTotal;
	}
	public int getRecordCount() {
		return recordCount;
	}
	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}
	public Double getAverageTMAX() {
		return averageTMAX;
	}
	public void setAverageTMAX(Double averageTMAX) {
		this.averageTMAX = averageTMAX;
	}
	@Override
	public String toString() {
		return "Station [stationID=" + stationID + ", tmaxTotal=" + tmaxTotal + ", recordCount=" + recordCount
				+ ", averageTMAX=" + averageTMAX + "]";
	}
	
	// A method to set tmaxTotal and record count while holding a lock 
	public synchronized void setAttributes(Double tmaxTotal, int recordCount){
		this.recordCount = recordCount;
		this.tmaxTotal = tmaxTotal;
	}
	

	// A method to set tmaxTotal and record count while holding a lock with a delay
	public synchronized void setAttributesWithDelay(Double tmaxTotal, int recordCount){
		Fibonacci.calculateFibonacci(17);
		this.recordCount = recordCount;
		this.tmaxTotal = tmaxTotal;
	}
	
	
}
