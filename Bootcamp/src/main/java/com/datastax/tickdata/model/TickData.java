package com.datastax.tickdata.model;

import org.json.JSONException;

import com.datastax.commons.EventObject;

public class TickData {
	private EventObject tick = new EventObject();

	private TickData(Builder aTick) {
		try {
			tick.setEventName("Tick");
			tick.put("Symbol", aTick.symbol);
			tick.put("Price", aTick.price);
			tick.put("Shares",  aTick.shares);
			tick.put("SeqNo", aTick.seqNo);
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	public static class Builder {
		String symbol;
		Double price = 0.0;
		long shares = 0;
		long seqNo = 0;
		
		public Builder symbol(String symbol) {
			this.symbol = symbol;
			return this;
		}

		public Builder price(Double price) {
			this.price = price;
			return this;
		}
		
		public Builder shares(long shares) {
			this.shares = shares;
			return this;
		}
		
		public Builder seqNo(long seqNo) {
			this.seqNo = seqNo;
			return this;
		}

		public TickData build() {
			return new TickData(this);
		}
	}

	public String getSymbol() throws JSONException {
		return tick.getString("Symbol");
	}

	public double getPrice() throws JSONException {
		return tick.getDouble("Price");
	}
	
	public void setSymbole(String symbol) throws JSONException {
		tick.put("Symbol",  symbol);
	}

	public void setPrice(Double price) throws JSONException {
		tick.put("Price", price);
	}
	
	@Override
	public String toString() {
		return tick.toString();
	}
}
