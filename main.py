import os
import sys
import time

# 引入各个模块的主函数
#以此方式引入前提是这些文件都在同级目录下
try:
    import update_data
    import build_dataset
    import check_data_quality
    import find_duplicates
    import run_gap_report
    import add_ticker
except ImportError as e:
    print(f"❌ 错误: 缺少必要的模块文件。请确保 update_data.py 等都在当前目录下。\nDetails: {e}")
    sys.exit(1)

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def print_header():
    print("="*60)
    print("      YFDATA 数据中台管理系统 (Data Middle Platform)")
    print("="*60)

def menu():
    while True:
        clear_screen()
        print_header()
        print("\n[ 核心流程 ]")
        print("  1. 🔄  全量/增量更新 (Update Data)")
        print("  2. 🏗️   构建数据矩阵 (Build Dataset -> csv)")
        print("  3. 🚀  [一键] 每日收盘流程 (Update + Build)")

        print("\n[ 资产管理 ]")
        print("  4. ➕  添加新资产 (Add Ticker)")
        print("  5. 🔪  重复资产去重 (Find Duplicates)")
        
        print("\n[ 质量监控 ]")
        print("  6. 🏥  数据健康体检 (Quality Check)")
        print("  7. 🔍  数据缺口诊断 (Gap Report)")

        print("\n[ 系统 ]")
        print("  0. 🚪  退出")
        print("-" * 60)
        
        choice = input("请选择操作 (0-7): ").strip()

        if choice == '1':
            print("\n>>> 启动数据更新...")
            update_data.main()
            input("\n按回车键继续...")
            
        elif choice == '2':
            print("\n>>> 启动矩阵构建...")
            build_dataset.main()
            input("\n按回车键继续...")
            
        elif choice == '3':
            print("\n>>> 启动每日流程...")
            update_data.main()
            print("\n-----------------------------------")
            build_dataset.main()
            print("\n✅ 每日流程完成！")
            input("\n按回车键继续...")
            
        elif choice == '4':
            print("\n>>> 启动资产录入...")
            add_ticker.main() # 进入交互模式
            
        elif choice == '5':
            print("\n>>> 启动相关性分析...")
            find_duplicates.main()
            input("\n按回车键继续...")
            
        elif choice == '6':
            print("\n>>> 启动基础体检...")
            check_data_quality.main()
            input("\n按回车键继续...")

        elif choice == '7':
            print("\n>>> 启动缺口深度诊断...")
            run_gap_report.main()
            input("\n按回车键继续...")

        elif choice == '0':
            print("再见！")
            sys.exit(0)
        else:
            print("无效输入，请重试。")
            time.sleep(1)

if __name__ == "__main__":
    menu()