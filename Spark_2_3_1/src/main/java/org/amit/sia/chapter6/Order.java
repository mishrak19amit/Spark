package org.amit.sia.chapter6;

import java.sql.Timestamp;

public class Order {

	private  Timestamp time;
	private  int orderId;
	private  int clientId;
	private  String symbol;
	private  int amount;
	private  Double price;
	private  String buy;
	public Timestamp getTime() {
		return time;
	}
	public void setTime(Timestamp time) {
		this.time = time;
	}
	public int getOrderId() {
		return orderId;
	}
	public void setOrderId(int orderId) {
		this.orderId = orderId;
	}
	public int getClientId() {
		return clientId;
	}
	public void setClientId(int clientId) {
		this.clientId = clientId;
	}
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public int getAmount() {
		return amount;
	}
	public void setAmount(int amount) {
		this.amount = amount;
	}
	public Double getPrice() {
		return price;
	}
	public void setPrice(Double price) {
		this.price = price;
	}
	public String isBuy() {
		return buy;
	}
	public void setBuy(String buy) {
		this.buy = buy;
	}
}
