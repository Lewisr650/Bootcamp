package com.datastax.tickdata.producer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import com.datastax.tickdata.model.TickData;

public class TickGenerator {

	private long TOTAL_TICKS = 0L;
	private List<TickValue> tickValueList = new ArrayList<TickValue>();

	public TickGenerator(List<String> exchangeSymbols) {
		int count = 1;
		for (String symbol : exchangeSymbols) {
			tickValueList.add(new TickValue(symbol, count++));
		}
	}

	public long getTicksGenerated() {
		return TOTAL_TICKS;
	}

	public void generatorTicks(BlockingQueue<List<TickData>> queueTickData,
			long noOfTicks) {

		List<TickData> flusher = new ArrayList<TickData>();

		for (int i = 0; i < noOfTicks; i++) {
			TickValue tickValue = getTickValueRandom();
			flusher.add(new TickData.Builder().price(tickValue.price).symbol(tickValue.symbol).build());
			TOTAL_TICKS++;
			if (i % 20 == 0) {
				try {
					queueTickData.put(new ArrayList<TickData>(flusher));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				flusher.clear();
			}

			if (i % 10000 == 0) {
				sleepMillis(10);
			}
		}
	}

	private void sleepMillis(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	TickValue getTickValueRandom() {
		TickValue tickValue = tickValueList
				.get((int) (Math.random() * tickValueList.size()));
		tickValue.price = this.createRandomValue(tickValue.price);
		return tickValue;
	}

	class TickValue implements Serializable {
		private static final long serialVersionUID = 215526630751330868L;
		String symbol;
		double price;

		public TickValue(String tickSymbol, double value) {
			super();
			this.symbol = tickSymbol;
			this.price = value;
		}
	}

	private double createRandomValue(double lastValue) {
		double up = Math.random() * 2;
		double percentMove = (Math.random() * 1.0) / 100;

		if (up < 1) {
			lastValue -= percentMove;
		} else {
			lastValue += percentMove;
		}
		return lastValue;
	}
}
