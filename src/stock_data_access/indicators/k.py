"""Recursive K-line (Ki) indicator used by the index regime labeler."""

from __future__ import annotations

import pandas as pd


def calculate_k_indicator(close_prices: pd.Series, start_date: str, alpha: float = 0.967) -> pd.Series:
    """
    按日递归计算牛熊市指标 Ki (Pandas 向量化高效版)。

    算法规则:
    1. K1 等于开始计算的第一天前五日的收盘价均线。
    2. 第一天过后 (即 i >= 2 时)，Ki = (K_{i-1}) * alpha + Pi * (1 - alpha)。
       如果 alpha = 0.967，则公式等价于 Ki = (K_{i-1}) * 0.967 + Pi * 0.033。

    参数:
    close_prices: pd.Series, 历史收盘价序列（索引为日期/时间戳，值为收盘价），需按时间升序排列。
    start_date: str, 开始计算的第一天（对应 i=1）。
    alpha: float, 衰减系数（过去值的权重），默认 0.967 (fast)，0.983 为 slow。

    返回:
    pd.Series: 从第一天开始计算的 K 值序列，其索引与输入序列中 start_date 及其之后的日期对应。
    """
    # 确保时间序列是按升序排列的
    close_prices = close_prices.sort_index()

    # 检查开始日期是否存在于索引中，支持字符串日期和Timestamp对象的转换
    if start_date in close_prices.index:
        start_ts = start_date
    else:
        start_ts = pd.to_datetime(start_date)
        if start_ts not in close_prices.index:
            # 尝试寻找等于或大于start_date的第一个有效交易日
            idx_ts = pd.to_datetime(close_prices.index)
            future_mask = idx_ts >= start_ts
            if not future_mask.any():
                raise ValueError(f"价格数据中找不到设定的开始日期及之后的日期：{start_date}")
            start_ts = close_prices.index[future_mask][0]

    start_idx = close_prices.index.get_loc(start_ts)

    # 确保第一天前面至少有 5 天的历史数据用于计算初始 5 日均线
    if start_idx < 5:
        raise ValueError("开始日期前的数据不足 5 天，无法计算初始的 5 日均线！请提供更早的历史数据。")

    # 1. 计算 K1：第一天前 5 日的收盘价均线 (不包含第一天当天)
    k1 = close_prices.iloc[start_idx - 5 : start_idx].mean()

    # 2. 构造用于 ewm 递归的序列（截取从开始日期到最后的数据）
    target_prices = close_prices.iloc[start_idx:].copy(deep=True)

    # 第一天的实际价格用 k1 替换，作为初始种子
    target_prices.iloc[0] = k1

    # 3. 使用 ewm 递归计算后续的 Ki
    # Pandas ewm 中的 alpha 参数指的是当前价格 Pi 的权重，即 1 - 传入的 alpha
    # 比如 alpha = 0.8, ewm_alpha = 0.2
    ewm_alpha = 1.0 - alpha
    k_values = target_prices.ewm(alpha=ewm_alpha, adjust=False).mean()

    result_series = k_values.rename("K_Value")
    result_series.index.name = "date"
    return result_series


__all__ = ["calculate_k_indicator"]
