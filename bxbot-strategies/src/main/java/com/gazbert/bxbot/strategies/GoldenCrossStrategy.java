package com.gazbert.bxbot.strategies;

import com.gazbert.bxbot.strategy.api.StrategyConfig;
import com.gazbert.bxbot.strategy.api.StrategyException;
import com.gazbert.bxbot.strategy.api.TradingStrategy;
import com.gazbert.bxbot.trading.api.Market;
import com.gazbert.bxbot.trading.api.TradingApi;

/**
 * The "Golden cross" is when a shorter period SMA crosses either
 * above or below a longer period moving average.
 *
 * <p>
 *     e.g.,
 *      if sma(50) > sma(200)
 *          buy
 * </p>
 *
 */
public class GoldenCrossStrategy implements TradingStrategy {

	private TradingApi tradingApi;
	private Market market;

	@Override
	public void init(TradingApi tradingApi, Market market, StrategyConfig config) {
		this.tradingApi = tradingApi;
		this.market = market;
	}

	@Override
	public void execute() throws StrategyException {

	}
}
