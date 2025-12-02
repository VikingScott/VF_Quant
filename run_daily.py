import update_data
import build_dataset
import time
import sys

def main():
    start_time = time.time()
    print("="*50)
    print(f"🚀 [Auto] yfdata 每日更新任务启动")
    print(f"🕒 时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)

    # 1. 下载/更新数据
    print("\nStep 1: 更新数据仓库 (Data Lake Update)...")
    try:
        update_data.main()
    except Exception as e:
        print(f"❌ 更新步骤失败: {e}")
        sys.exit(1)

    # 2. 生成矩阵文件
    print("\nStep 2: 生成策略矩阵 (Matrix Building)...")
    try:
        build_dataset.main()
    except Exception as e:
        print(f"❌ 构建步骤失败: {e}")
        sys.exit(1)

    elapsed = time.time() - start_time
    print("\n" + "="*50)
    print(f"✅ 所有任务完成！耗时: {elapsed:.2f} 秒")
    print(f"📂 输出位置: data/processed/")
    print("="*50)

if __name__ == "__main__":
    main()