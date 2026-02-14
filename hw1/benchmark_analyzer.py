import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import argparse
import os
from datetime import datetime

def analyze_benchmark_results(csv_file, output_dir="benchmark_plots"):
    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found")
        return
    
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Reading benchmark data from {csv_file}")
    
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    print(f"Total operations: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    
    operation_counts = df['operation'].value_counts()
    print("\nOperation counts:")
    for op, count in operation_counts.items():
        print(f"  {op}: {count}")
    
    time_stats = df.groupby('operation')['time_us'].describe()
    print("\nTime statistics by operation (microseconds):")
    print(time_stats)
    
    plt.style.use('seaborn-v0_8')
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('LSM-Tree Benchmark Analysis', fontsize=16, fontweight='bold')
    
    axes[0, 0].pie(operation_counts.values, labels=operation_counts.index, autopct='%1.1f%%')
    axes[0, 0].set_title('Operation Distribution')
    
    operations = df['operation'].unique()
    avg_times = []
    
    for op in operations:
        op_data = df[df['operation'] == op]['time_us']
        non_zero_times = op_data[op_data > 0]
        if len(non_zero_times) > 0:
            avg_times.append(non_zero_times.mean())
        else:
            avg_times.append(0)
    
    bars = axes[0, 1].bar(operations, avg_times)
    axes[0, 1].set_title('Average Time by Operation (Non-Zero Only)')
    axes[0, 1].set_ylabel('Time (microseconds)')
    axes[0, 1].tick_params(axis='x', rotation=45)
    
    for bar, value in zip(bars, avg_times):
        axes[0, 1].text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                       f'{value:.1f}μs', ha='center', va='bottom')
    
    df_sorted = df.sort_index()
    df_sorted['cumulative_ops'] = range(1, len(df_sorted) + 1)
    
    for op in operations:
        op_data = df_sorted[df_sorted['operation'] == op]
        if len(op_data) > 0:
            axes[1, 0].scatter(op_data['cumulative_ops'], op_data['time_us'], 
                              label=op, alpha=0.6, s=10)
    
    axes[1, 0].set_title('Execution Time Progression')
    axes[1, 0].set_xlabel('Operation Number')
    axes[1, 0].set_ylabel('Time (microseconds)')
    axes[1, 0].legend()
    axes[1, 0].grid(True, alpha=0.3)
    
    cumulative_time = df.groupby('operation')['time_us'].cumsum()
    df_sorted['cumulative_time'] = cumulative_time
    
    for op in operations:
        op_data = df_sorted[df_sorted['operation'] == op]
        if len(op_data) > 0:
            axes[1, 1].plot(op_data['cumulative_ops'], op_data['cumulative_time'] / 1000000.0,
                           label=op, linewidth=2)

    axes[1, 1].set_title('Cumulative Time by Operation')
    axes[1, 1].set_xlabel('Operation Number')
    axes[1, 1].set_ylabel('Cumulative Time (seconds)')
    axes[1, 1].legend()
    axes[1, 1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plot_file = os.path.join(output_dir, 'benchmark_summary.png')
    plt.savefig(plot_file, dpi=300, bbox_inches='tight')
    print(f"\nSummary plot saved to: {plot_file}")
    
    for operation in operations:
        op_data = df[df['operation'] == operation]
        
        if len(op_data) > 0:
            fig, axes = plt.subplots(1, 2, figsize=(12, 5))
            fig.suptitle(f'{operation} Operation Analysis', fontsize=14, fontweight='bold')
            
            axes[0].hist(op_data['time_us'], bins=50, alpha=0.7, edgecolor='black')
            axes[0].set_title(f'{operation} Time Distribution')
            axes[0].set_xlabel('Time (microseconds)')
            axes[0].set_ylabel('Frequency')
            axes[0].grid(True, alpha=0.3)
            
            axes[1].plot(range(len(op_data)), op_data['time_us'], alpha=0.7)
            axes[1].set_title(f'{operation} Time Progression')
            axes[1].set_xlabel('Operation Instance')
            axes[1].set_ylabel('Time (microseconds)')
            axes[1].grid(True, alpha=0.3)
            
            plt.tight_layout()
            op_plot_file = os.path.join(output_dir, f'{operation.lower()}_analysis.png')
            plt.savefig(op_plot_file, dpi=300, bbox_inches='tight')
            print(f"{operation} analysis plot saved to: {op_plot_file}")
            plt.close()
    
    report_file = os.path.join(output_dir, 'benchmark_report.txt')
    with open(report_file, 'w') as f:
        f.write("LSM-Tree Benchmark Analysis Report\n")
        f.write(f"Analysis date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Data file: {csv_file}\n")
        f.write(f"Total operations: {len(df):,}\n\n")
        
        f.write("Operation Statistics:\n")
        for op, count in operation_counts.items():
            f.write(f"{op}: {count:,} operations ({count/len(df)*100:.1f}%)\n")
        
        f.write("\nTime Statistics (microseconds):\n")
        for op in operations:
            op_data = df[df['operation'] == op]['time_us']
            if len(op_data) > 0:
                f.write(f"\n{op}:\n")
                f.write(f"  Mean: {op_data.mean():.2f} μs\n")
                f.write(f"  Median: {op_data.median():.2f} μs\n")
                f.write(f"  Std Dev: {op_data.std():.2f} μs\n")
                f.write(f"  Min: {op_data.min():.2f} μs\n")
                f.write(f"  Max: {op_data.max():.2f} μs\n")
                f.write(f"  95th percentile: {op_data.quantile(0.95):.2f} μs\n")
        
        zero_times = df[df['time_us'] == 0]
        if len(zero_times) > 0:
            f.write(f"\nWarning: {len(zero_times):,} operations recorded 0 μs time\n")
            f.write("This may indicate timing measurement issues\n")
    
    print(f"\nDetailed report saved to: {report_file}")
    
    print("BENCHMARK ANALYSIS SUMMARY")
    print(f"Total operations analyzed: {len(df):,}")
    
    for op in operations:
        op_data = df[df['operation'] == op]['time_us']
        if len(op_data) > 0:
            non_zero_times = op_data[op_data > 0]
            print(f"\n{op}:")
            print(f"  Operations: {len(op_data):,}")
            print(f"  Non-zero times: {len(non_zero_times):,}")
            if len(non_zero_times) > 0:
                print(f"  Avg time (non-zero): {non_zero_times.mean():.2f} μs")
                print(f"  Max time: {non_zero_times.max():.2f} μs")
    
    print(f"\nPlots and report saved to: {output_dir}/")

def main():
    parser = argparse.ArgumentParser(description='Analyze LSM-tree benchmark results')
    parser.add_argument('csv_file', help='Path to benchmark CSV file')
    parser.add_argument('-o', '--output', default='benchmark_plots', 
                       help='Output directory for plots (default: benchmark_plots)')
    
    args = parser.parse_args()
    
    analyze_benchmark_results(args.csv_file, args.output)

if __name__ == "__main__":
    main()