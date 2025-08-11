import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from statsmodels.tsa.stattools import adfuller, kpss
from datetime import datetime
import matplotlib

matplotlib.use("TkAgg")  # Use "QtAgg" for PyQt users

# **1. Connect to MySQL**
conn = mysql.connector.connect(
    host="localhost",
    user="root",  # Change if needed
    password="1234",  # Change if needed
    database="btc_options_db"
)
cursor = conn.cursor()

# **2. Fetch SPD Skewness Data from MySQL**
def fetch_spd_skewness():
    sql = """
         SELECT timestamp, expiration_timestamp, atm_slope
            FROM btc_iv_spd_skewness
            WHERE timestamp >= UNIX_TIMESTAMP(NOW()-INTERVAL 0.5 DAY) * 1000
            ORDER BY timestamp DESC; 
    """
    cursor.execute(sql)
    rows = cursor.fetchall()

    if not rows:
        return None

    df = pd.DataFrame(rows, columns=['timestamp', 'expiration_timestamp', 'atm_slope'])
    df['timestamp'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(int(x)/1000))

    return df


# **3. Perform Stationarity Tests**
def test_stationarity(series):
    try:
        adf_result = adfuller(series.dropna(), autolag='AIC')
        kpss_result = kpss(series.dropna(), regression='c', nlags="auto")

        return {
            "ADF Statistic": adf_result[0],
            "ADF p-value": adf_result[1],
            "KPSS Statistic": kpss_result[0],
            "KPSS p-value": kpss_result[1]
        }
    except Exception as e:
        print(f"⚠️ Error in stationarity test: {e}")
        return None


# **4. Generate Unique Colors for Each Expiration**
def generate_colors(n):
    cmap = plt.get_cmap('tab10')  # Use distinct colors
    return [cmap(i) for i in range(n)]


# **5. Plot SPD Skewness with Stationarity Test & Standard Deviation**
def plot_skewness_with_std_dev():
    df = fetch_spd_skewness()
    if df is None or df.empty:
        print("⚠️ No SPD skewness data found in MySQL.")
        return

    plt.figure(figsize=(12, 6))
    expirations = df['expiration_timestamp'].unique()
    colors = generate_colors(len(expirations))

    # Create a list to hold legend handles (for skewness curves only)
    legend_handles = []

    for exp_ts, color in zip(expirations, colors):
        exp_df = df[df['expiration_timestamp'] == exp_ts].set_index('timestamp')
        print(exp_df)

        # Calculate mean and standard deviation
        mean_skewness = exp_df['atm_slope'].mean()
        std_dev = exp_df['atm_slope'].std()

        # Perform stationarity test
        stationarity_results = test_stationarity(exp_df['atm_slope'])
        modified_exp_ts = exp_ts
        modified_time_stamp = datetime.fromtimestamp(modified_exp_ts)


        # Print results
        print(f"\n📊 Stationarity Test for Expiry {modified_time_stamp.strftime('%d-%b-%Y')}:")
        print(stationarity_results)

        # Plot SPD Skewness & store the handle for the legend
        curve, = plt.plot(exp_df.index, exp_df['atm_slope'], linestyle='-', marker='o', color=color,
                          label=f"Exp: {modified_time_stamp.strftime('%d-%b-%Y')}")
        legend_handles.append(curve)  # Add only skewness curves to the legend

        # Plot Mean Line (No label for legend)
        plt.axhline(mean_skewness, color=color, linestyle='--', alpha=0.5)

        # Plot ±3 Standard Deviation Bands (No label for legend)
        plt.fill_between(exp_df.index,
                         mean_skewness - 2.5 * std_dev,
                         mean_skewness + 2.5 * std_dev,
                         color=color, alpha=0.2)

    plt.xlabel("Time")
    plt.ylabel("ATM Slope")
    plt.title("ATM Slope Over Time with Mean & ±5 Std Dev Bands")
    plt.legend(handles=legend_handles, loc="lower right")  # ✅ Only skewness curves in legend
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.show()



# **8. Run Static or Real-Time Graph**
if __name__ == "__main__":
    plot_skewness_with_std_dev()