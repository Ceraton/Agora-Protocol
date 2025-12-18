import backtrader as bt
import os
import sys
import pandas as pd
import asyncio
from typing import List, Dict, Any
from strategy.bollinger_strategy import BollingerStrategy
from data.data_storage import DATA_DIR
import datetime

class CouncilStrategy(bt.Strategy):
    """
    Backtrader strategy that wraps the MetaStrategy (Council of AIs).
    """
    params = dict(
        meta_strategy=None, # MetaStrategy instance
    )

    def __init__(self):
        self.meta = self.p.meta_strategy
        self.dataclose = self.datas[0].close
        self.order = None

    def next(self):
        if self.order:
            return

        # Prepare candle data for the council
        candle = {
            'timestamp': self.datas[0].datetime.datetime(0),
            'open': self.datas[0].open[0],
            'high': self.datas[0].high[0],
            'low': self.datas[0].low[0],
            'close': self.datas[0].close[0],
            'volume': self.datas[0].volume[0]
        }

        # Run async MetaStrategy in sync context
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if loop.is_running():
            # If loop is already running (e.g. from verify script or dashboard), 
            # we need to use a different approach.
            # For backtesting, we skip the 'async' part if we can or use nest_asyncio.
            # Since we can't install nest_asyncio here easily, let's try a simpler wrapper.
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, self.meta.on_candle(candle))
                signal = future.result()
        else:
            signal = loop.run_until_complete(self.meta.on_candle(candle))

        if signal:
            if signal['side'] == 'buy' and not self.position:
                self.order = self.buy()
            elif signal['side'] == 'sell' and self.position:
                self.order = self.sell()

    def notify_order(self, order):
        if order.status in [order.Completed]:
            self.order = None

class BacktestEngine:
    def __init__(self, start_cash=10000.0):
        self.cerebro = bt.Cerebro()
        self.start_cash = start_cash
        self.cerebro.broker.setcash(start_cash)
        self.cerebro.addsizer(bt.sizers.PercentSizer, percents=90) # Use 90% of cash
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        self.results = None

    def run(self, data_df: pd.DataFrame, strategy_class=BollingerStrategy, **kwargs):
        """
        Runs the backtest using a DataFrame.
        """
        data = bt.feeds.PandasData(dataname=data_df, datetime='timestamp')
        
        self.cerebro.adddata(data)
        self.cerebro.addstrategy(strategy_class, **kwargs)
        
        print(f'Starting Portfolio Value: {self.cerebro.broker.getvalue():.2f}')
        self.results = self.cerebro.run()
        print(f'Final Portfolio Value: {self.cerebro.broker.getvalue():.2f}')
        
        return self.results

    def get_metrics(self):
        if not self.results:
            return {}
        
        strat = self.results[0]
        return {
            'roi': (self.cerebro.broker.getvalue() / self.start_cash - 1) * 100,
            'sharpe': strat.analyzers.sharpe.get_analysis().get('sharperatio', 0),
            'max_drawdown': strat.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown', 0),
            'final_value': self.cerebro.broker.getvalue()
        }

if __name__ == "__main__":
    from data.data_storage import load_historical_data
    df = load_historical_data('BTC_USD_1h.csv')
    if not df.empty:
        engine = BacktestEngine()
        engine.run(df)
        print(engine.get_metrics())
