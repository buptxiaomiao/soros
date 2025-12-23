import backtrader as bt
import pandas as pd
import numpy as np
from datetime import datetime


# 1. 定义交易策略
class DualMASStrategy(bt.Strategy):
    """
    双均线交叉策略
    短期均线上穿长期均线时买入，下穿时卖出
    """
    params = (
        ('short_window', 10),  # 短期均线周期
        ('long_window', 30),  # 长期均线周期
    )

    def __init__(self):
        print("DualMASStrategy init once.")
        # 初始化技术指标
        self.short_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.short_window
        )
        self.long_ma = bt.indicators.SimpleMovingAverage(
            self.data.close, period=self.params.long_window
        )
        self.crossover = bt.indicators.CrossOver(self.short_ma, self.long_ma)

        # 记录交易订单
        self.order = None

    def next(self):
        # 如果有未完成订单，跳过本次判断
        if self.order:
            return

        hold_positions = {}
        for data, pos in self.broker.positions.items():
            if pos.size != 0:  # 只处理有持仓的标的
                hold_positions[data._name] = pos.size
                print(f"股票 {data._name}: 持仓 {pos.size} 股, 成本价 {pos.price:.2f}")

        print(f'self.position.size={self.position.size}')
        print(f'self.position.price={self.position.price}')
        # 金叉信号：短期均线上穿长期均线，且没有持仓
        if not self.position and self.crossover > 0:
            self.order = self.buy(size=100)  # 买入100股
            self.log(f'BUY CREATE, Price: {self.data.close[0]:.2f}')

        # 死叉信号：短期均线下穿长期均线，且有持仓
        elif self.position and self.crossover < 0:
            self.order = self.sell(size=100)  # 卖出全部持仓
            self.log(f'SELL CREATE, Price: {self.data.close[0]:.2f}')

    def log(self, txt, dt=None):
        '''日志记录函数'''
        dt = dt or self.datas[0].datetime.date(0)
        print(self.datas)
        print(self.datas[0])
        print(f'{dt.isoformat()}, {txt}')

    def notify_order(self, order):
        '''订单状态通知'''
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    f'买入执行, 价格: {order.executed.price:.2f}, 成本: {order.executed.value:.2f}, 佣金: {order.executed.comm:.2f}')
            else:
                self.log(
                    f'卖出执行, 价格: {order.executed.price:.2f}, 成本: {order.executed.value:.2f}, 佣金: {order.executed.comm:.2f}')
            self.order = None


# 2. 创建模拟数据（实际使用时替换为真实数据）
def create_sample_data():
    """生成模拟股票数据"""
    dates = pd.date_range(start='2020-01-01', end='2023-12-31', freq='D')
    n_days = len(dates)

    # 生成随机价格序列（几何布朗运动）
    np.random.seed(42)
    returns = np.random.normal(0.0005, 0.02, n_days)
    prices = 100 * np.exp(np.cumsum(returns))

    # 添加周末效应和波动率聚集
    prices = prices * (1 + 0.01 * np.sin(np.arange(n_days) * 2 * np.pi / 7))

    df = pd.DataFrame({
        'open': prices * (1 + np.random.normal(0, 0.01, n_days)),
        'high': prices * (1 + np.abs(np.random.normal(0, 0.015, n_days))),
        'low': prices * (1 - np.abs(np.random.normal(0, 0.015, n_days))),
        'close': prices,
        'volume': np.random.lognormal(10, 1, n_days)
    }, index=dates)

    return df


# 3. 主回测函数
def run_backtest():
    '''运行完整的回测流程'''

    # 创建Cerebro引擎
    cerebro = bt.Cerebro()

    # 设置初始资金
    initial_cash = 100000.0
    cerebro.broker.setcash(initial_cash)

    # 设置交易佣金（A股通常为万分之3）
    cerebro.broker.setcommission(commission=0.0003)

    # 添加策略
    cerebro.addstrategy(DualMASStrategy, short_window=5, long_window=20)

    # 加载数据
    sample_data = create_sample_data()
    data = bt.feeds.PandasData(dataname=sample_data)
    cerebro.adddata(data)

    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')

    # 运行回测
    print('=' * 50)
    print('开始回测...')
    print(f'初始资金: {initial_cash:,.2f}')

    results = cerebro.run()
    strategy = results[0]

    # 输出结果
    final_value = cerebro.broker.getvalue()
    print(f'最终资金: {final_value:,.2f}')
    print(f'绝对收益: {(final_value / initial_cash - 1) * 100:.2f}%')

    # 分析器结果
    sharpe_ratio = strategy.analyzers.sharpe.get_analysis()
    drawdown = strategy.analyzers.drawdown.get_analysis()
    returns = strategy.analyzers.returns.get_analysis()
    trade_analysis = strategy.analyzers.trades.get_analysis()

    print('\n=== 绩效分析 ===')
    print(f"夏普比率: {sharpe_ratio['sharperatio']:.2f}")
    print(f"最大回撤: {drawdown['max']['drawdown']:.2f}%")
    print(f"年化收益: {returns['rnorm100']:.2f}%")

    if 'total' in trade_analysis:
        print(f"总交易次数: {trade_analysis['total']['total']}")
        if trade_analysis['total']['total'] > 0:
            print(f"盈利交易比例: {trade_analysis['won']['total'] / trade_analysis['total']['total'] * 100:.1f}%")

    # 绘制图表
    print('\n生成回测图表...')
    cerebro.plot(style='candlestick', volume=True)


# 4. 进阶功能示例
class AdvancedStrategy(bt.Strategy):
    """进阶策略示例：包含止损止盈和仓位管理"""
    params = (
        ('fast', 10),
        ('slow', 30),
        ('stop_loss', 0.03),  # 3%止损
        ('take_profit', 0.06),  # 6%止盈
    )

    def __init__(self):
        self.fast_ma = bt.indicators.EMA(self.data.close, period=self.params.fast)
        self.slow_ma = bt.indicators.EMA(self.data.close, period=self.params.slow)
        self.crossover = bt.indicators.CrossOver(self.fast_ma, self.slow_ma)
        self.order = None

    def next(self):
        if self.order:
            return

        if not self.position:
            if self.crossover > 0:
                # 使用市价单的80%资金买入
                size = int(self.broker.getcash() * 0.8 / self.data.close[0])
                if size > 0:
                    self.order = self.buy(size=size)
                    self.stop_price = self.data.close[0] * (1 - self.params.stop_loss)
                    self.take_profit_price = self.data.close[0] * (1 + self.params.take_profit)
        else:
            # 止损止盈逻辑
            if self.data.close[0] <= self.stop_price or self.data.close[0] >= self.take_profit_price:
                self.order = self.sell(size=self.position.size)

            # 死叉卖出
            elif self.crossover < 0:
                self.order = self.sell(size=self.position.size)


if __name__ == '__main__':
    # 运行基础回测
    run_backtest()