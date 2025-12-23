import numpy as np
import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
from scipy.fft import fft, ifft, fftfreq
import warnings

warnings.filterwarnings('ignore')


def fourier_quant_analysis(ticker='AAPL', start_date='2020-01-01', end_date='2023-12-31',
                           cutoff_freq_ratio=0.1, min_data_points=100):
    """
    傅里叶变换在量化分析中的综合应用

    参数:
    ticker: 股票代码
    start_date: 开始日期
    end_date: 结束日期
    cutoff_freq_ratio: 频率截断比率(用于噪声过滤)
    min_data_points: 最小数据点数要求
    """

    try:
        # ===== 1. 数据获取与预处理 =====
        print(f"正在获取 {ticker} 股票数据...")
        stock_data = yf.download(ticker, start=start_date, end=end_date)

        if stock_data.empty:
            raise ValueError("获取的数据为空，请检查股票代码和日期范围")

        prices = stock_data['Adj Close']

        # 数据清洗
        initial_count = len(prices)
        prices = prices.dropna()
        cleaned_count = len(prices)

        if cleaned_count == 0:
            raise ValueError("数据清洗后无有效数据")

        if cleaned_count < min_data_points:
            raise ValueError(f"数据点数量({cleaned_count})少于要求的最小值({min_data_points})")

        print(f"数据预处理完成: 原始数据{initial_count}条, 有效数据{cleaned_count}条")

        # ===== 2. 傅里叶变换与周期识别 =====
        print("正在进行傅里叶变换分析...")

        # 确保数据是一维数组
        price_values = prices.values.reshape(-1)
        N = len(price_values)

        # 执行FFT
        fft_result = fft(price_values)
        frequencies = fftfreq(N, d=1)  # 假设日数据，d=1天

        # 只考虑正频率部分（排除直流分量）
        positive_mask = frequencies > 0
        positive_freqs = frequencies[positive_mask]
        amplitudes = np.abs(fft_result[positive_mask])

        # 找到主导周期
        if len(amplitudes) > 0:
            dominant_idx = np.argmax(amplitudes)
            dominant_freq = positive_freqs[dominant_idx]
            dominant_period = 1.0 / dominant_freq if dominant_freq != 0 else np.inf
            dominant_amplitude = amplitudes[dominant_idx]
        else:
            raise ValueError("无法计算有效的频率成分")

        # ===== 3. 趋势平滑（噪声过滤） =====
        # 设置频率截断阈值
        cutoff_idx = int(N * cutoff_freq_ratio)

        # 复制频谱并进行滤波
        fft_filtered = fft_result.copy()
        # 将高频部分置零（注意处理对称性）
        fft_filtered[cutoff_idx + 1:N - cutoff_idx] = 0

        # 逆傅里叶变换得到平滑后的价格
        smoothed_prices = np.real(ifft(fft_filtered))

        # ===== 4. 结果可视化 =====
        plt.figure(figsize=(15, 10))

        # 子图1: 原始价格与平滑价格
        plt.subplot(3, 1, 1)
        plt.plot(prices.index, price_values, label='原始价格', alpha=0.7, linewidth=1)
        plt.plot(prices.index, smoothed_prices, label='平滑趋势', color='red', linewidth=2)
        plt.title(f'{ticker} - 股价走势与傅里叶平滑')
        plt.ylabel('价格 (USD)')
        plt.legend()
        plt.grid(True, alpha=0.3)

        # 子图2: 振幅频谱
        plt.subplot(3, 1, 2)
        # 转换为周期（天）并显示主要周期范围
        period_range = 1.0 / positive_freqs
        display_range = period_range <= 365  # 显示1年以内的周期

        plt.semilogy(period_range[display_range], amplitudes[display_range])
        plt.axvline(x=dominant_period, color='red', linestyle='--',
                    label=f'主导周期: {dominant_period:.1f}天')
        plt.title('振幅频谱 (频域分析)')
        plt.xlabel('周期 (天)')
        plt.ylabel('振幅 (对数坐标)')
        plt.legend()
        plt.grid(True, alpha=0.3)

        # 子图3: 重构的主导周期成分
        plt.subplot(3, 1, 3)
        # 重构主导周期信号
        dominant_component = dominant_amplitude * np.cos(
            2 * np.pi * dominant_freq * np.arange(N) +
            np.angle(fft_result[positive_mask][dominant_idx])
        )

        plt.plot(prices.index, dominant_component, color='green', linewidth=2)
        plt.title('重构的主导周期成分')
        plt.xlabel('日期')
        plt.ylabel('信号强度')
        plt.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.show()

        # ===== 5. 分析结果输出 =====
        print("\n" + "=" * 50)
        print("傅里叶分析结果总结")
        print("=" * 50)
        print(f"分析标的: {ticker}")
        print(f"分析周期: {start_date} 至 {end_date}")
        print(f"主导周期: {dominant_period:.2f} 天")
        print(f"主导频率: {dominant_freq:.6f} 1/天")
        print(f"周期振幅: {dominant_amplitude:.2f}")
        print(f"噪声过滤: 保留最低{cutoff_freq_ratio * 100}%的频率成分")

        # 识别其他显著周期（振幅大于最大振幅的50%）
        significant_periods = []
        threshold = dominant_amplitude * 0.5

        for i in range(len(amplitudes)):
            if amplitudes[i] > threshold and i != dominant_idx:
                period = 1.0 / positive_freqs[i]
                if 5 <= period <= 365:  # 只关注5天到1年内的周期
                    significant_periods.append((period, amplitudes[i]))

        if significant_periods:
            significant_periods.sort(key=lambda x: x[1], reverse=True)
            print("\n其他显著周期:")
            for period, amp in significant_periods[:3]:  # 显示前3个显著周期
                print(f"  - {period:.1f}天周期 (相对强度: {amp / dominant_amplitude:.2f})")

        return {
            'prices': prices,
            'smoothed_prices': smoothed_prices,
            'dominant_period': dominant_period,
            'dominant_frequency': dominant_freq,
            'significant_periods': significant_periods,
            'frequencies': positive_freqs,
            'amplitudes': amplitudes
        }

    except Exception as e:
        print(f"分析过程中出现错误: {str(e)}")
        return None


def rolling_fourier_analysis(ticker='AAPL', window_size=252, step_size=63):
    """
    滚动窗口傅里叶分析 - 检测周期性的时变特征
    """
    try:
        # 获取更长时间范围的数据
        stock_data = yf.download(ticker, start='2018-01-01', end='2023-12-31')
        prices = stock_data['Adj Close'].dropna()

        periods = []
        dates = []

        # 滚动窗口分析
        for i in range(0, len(prices) - window_size, step_size):
            window_data = prices.iloc[i:i + window_size]

            if len(window_data) < window_size:
                continue

            # 计算当前窗口的主导周期
            fft_result = fft(window_data.values)
            frequencies = fftfreq(len(window_data), d=1)

            positive_mask = frequencies > 0
            positive_freqs = frequencies[positive_mask]
            amplitudes = np.abs(fft_result[positive_mask])

            if len(amplitudes) > 0:
                dominant_idx = np.argmax(amplitudes)
                dominant_period = 1.0 / positive_freqs[dominant_idx] if positive_freqs[dominant_idx] != 0 else 0

                # 只记录合理的周期
                if 5 <= dominant_period <= 365:
                    periods.append(dominant_period)
                    dates.append(window_data.index[-1])

        # 绘制周期变化图
        if periods:
            plt.figure(figsize=(12, 6))
            plt.plot(dates, periods, marker='o', markersize=2)
            plt.title(f'{ticker} - 主导周期随时间变化')
            plt.ylabel('主导周期 (天)')
            plt.xlabel('日期')
            plt.grid(True, alpha=0.3)
            plt.show()

            print(f"周期变化范围: {min(periods):.1f} - {max(periods):.1f} 天")
            print(f"平均周期: {np.mean(periods):.1f} 天")

        return periods, dates

    except Exception as e:
        print(f"滚动分析错误: {str(e)}")
        return [], []


# ===== 主执行部分 =====
if __name__ == "__main__":
    # 示例1: 基本分析
    results = fourier_quant_analysis(ticker='AAPL', cutoff_freq_ratio=0.08)

    # 示例2: 滚动窗口分析（检测周期性变化）
    if results is not None:
        print("\n正在进行滚动窗口分析...")
        periods, dates = rolling_fourier_analysis(ticker='AAPL')