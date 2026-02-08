# coding: utf-8
"""
对比两个CSV文件中相同股票代码的板块名称差异
"""
import pandas as pd


def compare_bk_names(file1, file2):
    """
    对比两个CSV文件中相同股票代码的板块名称差异
    """
    # 读取CSV文件
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    print(f"文件1: {file1}")
    print(f"  总行数: {len(df1)}")
    print(f"  股票代码数: {df1['股票代码m'].nunique()}")

    print(f"\n文件2: {file2}")
    print(f"  总行数: {len(df2)}")
    print(f"  股票代码数: {df2['股票代码m'].nunique()}")

    # 提取股票代码和板块名称映射
    bk_map1 = df1.set_index('股票代码m')['板块名称'].to_dict()
    bk_map2 = df2.set_index('股票代码m')['板块名称'].to_dict()

    # 找出共同的股票代码
    common_codes = set(bk_map1.keys()) & set(bk_map2.keys())
    print(f"\n共同的股票代码数: {len(common_codes)}")

    # 对比板块名称差异
    differences = []
    for code in common_codes:
        bk1 = bk_map1[code]
        bk2 = bk_map2[code]
        if bk1 != bk2:
            differences.append({
                '股票代码m': code,
                '文件1_板块名称': bk1,
                '文件2_板块名称': bk2
            })

    print(f"\n板块名称不同的股票数: {len(differences)}")

    if differences:
        diff_df = pd.DataFrame(differences)
        print("\n" + "=" * 80)
        print("板块名称差异明细:")
        print("=" * 80)
        print(diff_df.to_string(index=False))

        # 统计差异类型
        print("\n" + "=" * 80)
        print("差异统计:")
        print("=" * 80)
        print(diff_df['文件1_板块名称'].value_counts().head(20))

    # 找出仅存在于一个文件中的股票
    only_in_file1 = set(bk_map1.keys()) - set(bk_map2.keys())
    only_in_file2 = set(bk_map2.keys()) - set(bk_map1.keys())

    if only_in_file1:
        print(f"\n仅存在于文件1的股票数: {len(only_in_file1)}")
        print(f"  示例: {list(only_in_file1)[:10]}")

    if only_in_file2:
        print(f"\n仅存在于文件2的股票数: {len(only_in_file2)}")
        print(f"  示例: {list(only_in_file2)[:10]}")

    return differences


if __name__ == '__main__':
    file1 = 'tx_bk_sw_hy1_stocks_rt.csv'
    file2 = 'tx_bk_sw_hy2_stocks_rt.csv'

    compare_bk_names(file1, file2)
