#!/usr/bin/env python3
"""
快速排序算法实现

快速排序是一种高效的排序算法，采用分治法策略。
平均时间复杂度：O(n log n)
最坏时间复杂度：O(n^2)（当数组已排序或逆序时）

Author: spame
Date: 2026-03-02
"""

def quick_sort(arr):
    """
    快速排序主函数

    Args:
        arr: 需要排序的列表

    Returns:
        排序后的新列表
    """
    # 递归终止条件
    if len(arr) <= 1:
        return arr

    # 选择基准值（这里选择中间元素）
    pivot = arr[len(arr) // 2]

    # 分区：小于基准的放在左边，大于基准的放在右边
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]

    # 递归排序并合并
    return quick_sort(left) + middle + quick_sort(right)


def quick_sort_inplace(arr, low=0, high=None):
    """
    原地快速排序（空间效率更高）

    Args:
        arr: 需要排序的列表（会被原地修改）
        low: 起始索引
        high: 结束索引
    """
    if high is None:
        high = len(arr) - 1

    if low < high:
        # 分区并获取基准值的最终位置
        pivot_index = partition(arr, low, high)

        # 递归排序左右两部分
        quick_sort_inplace(arr, low, pivot_index - 1)
        quick_sort_inplace(arr, pivot_index + 1, high)


def partition(arr, low, high):
    """
    分区函数：选择基准值并将数组分为两部分

    Args:
        arr: 数组
        low: 起始索引
        high: 结束索引

    Returns:
        基准值最终位置的索引
    """
    # 选择最后一个元素作为基准值
    pivot = arr[high]

    # i 指向小于基准值的最后一个元素
    i = low - 1

    for j in range(low, high):
        if arr[j] <= pivot:
            i += 1
            arr[i], arr[j] = arr[j], arr[i]  # 交换

    # 将基准值放到正确的位置
    arr[i + 1], arr[high] = arr[high], arr[i + 1]
    return i + 1


def test_quick_sort():
    """测试快速排序"""
    # 测试用例
    test_cases = [
        [],
        [1],
        [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5],
        [5, 4, 3, 2, 1],
        [1, 2, 3, 4, 5],
        [3, 3, 3, 3],
        [-1, -2, -3, 0, 1, 2],
    ]

    print("=== 快速排序测试 ===\n")

    for i, case in enumerate(test_cases, 1):
        print(f"测试用例 {i}: {case}")

        # 测试非原地排序
        result = quick_sort(case.copy())
        print(f"  非原地排序结果: {result}")
        assert result == sorted(case), f"排序错误！期望 {sorted(case)}, 得到 {result}"

        # 测试原地排序
        arr_copy = case.copy()
        quick_sort_inplace(arr_copy)
        print(f"  原地排序结果: {arr_copy}")
        assert arr_copy == sorted(case), f"原地排序错误！期望 {sorted(case)}, 得到 {arr_copy}"

        print(f"  ✓ 通过\n")

    print("=== 所有测试通过！ ===")


if __name__ == "__main__":
    # 示例使用
    print("快速排序算法示例\n")

    # 非原地排序
    arr = [3, 6, 8, 10, 1, 2, 1]
    print(f"原始数组: {arr}")
    sorted_arr = quick_sort(arr)
    print(f"排序后数组: {sorted_arr}\n")

    # 原地排序
    arr = [3, 6, 8, 10, 1, 2, 1]
    print(f"原始数组: {arr}")
    quick_sort_inplace(arr)
    print(f"排序后数组: {arr}\n")

    # 运行测试
    test_quick_sort()
