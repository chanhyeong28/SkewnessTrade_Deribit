import tkinter as tk
from tkinter import ttk, messagebox
import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import CubicSpline
from datetime import datetime
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import logging

logging.basicConfig(
    level='DEBUG',
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# **1. Connect to MySQL**
def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="btc_options_db"
    )


# **2. Fetch Available Timestamps (Milliseconds to Human-Readable)**
def fetch_available_timestamps():
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT timestamp FROM btc_iv_spd_skewness ORDER BY timestamp DESC")
    rows = cursor.fetchall()
    conn.close()

    timestamps = []
    for row in rows:
        try:
            ts = int(row[0])
            formatted_time = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d %H:%M:%S')
            timestamps.append((formatted_time, ts))
        except Exception:
            continue

    return timestamps

# **3. Fetch Available Expiration Dates (Seconds to Human-Readable)**
def fetch_available_expirations():
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT expiration_timestamp FROM btc_iv_spd_skewness ORDER BY expiration_timestamp DESC LIMIT 10")
    rows = cursor.fetchall()
    conn.close()

    expirations = []
    for row in rows:
        try:
            exp_ts = int(row[0])
            formatted_exp = datetime.fromtimestamp(exp_ts).strftime('%Y-%m-%d')
            expirations.append((formatted_exp, exp_ts))
        except Exception:
            continue

    return expirations


# **4. Fetch SPD Data for Selected Time & Expiration**
def fetch_spd_data(selected_timestamp, selected_expiration):
    conn = connect_db()
    cursor = conn.cursor()

    try:
        unix_timestamp = int(selected_timestamp)
        exp_timestamp = int(selected_expiration)

        # Only considers a 1-day range
        sql = """
            SELECT strike_price, mark_iv, option_type, underlying_price
            FROM btc_options_tick 
            WHERE (%s - 86400000) <= timestamp
             AND timestamp <= %s 
             AND expiration_timestamp = %s
            ORDER BY timestamp DESC 
        """
        cursor.execute(sql, (unix_timestamp, unix_timestamp, exp_timestamp))
        rows = cursor.fetchall()
        logging.info(f'rows: {rows}')

        if not rows:
            return None, None, None

        df = pd.DataFrame(rows, columns=['strike_price', 'mark_iv', 'option_type', 'underlying_price'])
        logging.info(f'df: {df}')

        latest_underlying_price = df['underlying_price'].iloc[0] if not df.empty else None
        remaining_maturity = (exp_timestamp - (unix_timestamp // 1000)) / (365 * 24 * 60 * 60)

        return df if not df.empty else None, latest_underlying_price, remaining_maturity

    except Exception as e:
        print(f"⚠️ Error fetching SPD data: {e}")
        return None, None, None

    finally:
        conn.close()


# **5. Compute SPD & Volatility Curve**
def compute_spd(df, latest_underlying_price, remaining_maturity):
    risk_free_rate = 0.043

    call_data = df[df['option_type'] == 'call'].drop_duplicates('strike_price', keep='first').sort_values('strike_price')
    call_data['log_moneyness'] = np.log(call_data["strike_price"]/latest_underlying_price)
    put_data = df[df['option_type'] == 'put'].drop_duplicates('strike_price', keep='first').sort_values('strike_price')
    put_data['log_moneyness'] = np.log(put_data["strike_price"]/latest_underlying_price)


    if len(call_data) < 3 or len(put_data) < 3:
        return None, None, None, None

    try:
        iv_curve = pd.concat([put_data, call_data]).sort_values('log_moneyness')
        print(f'iv_curve: {iv_curve}')
        # **Compute average mark_iv for duplicate moneynesss**
        iv_curve = iv_curve.groupby('log_moneyness', as_index=False)['mark_iv'].mean()
        print(f'amount of data : {len(iv_curve)}')
        logging.debug(iv_curve)
        logging.debug(remaining_maturity)
        cs_iv_curve = CubicSpline(iv_curve['log_moneyness'].astype(float), iv_curve['mark_iv'].astype(float), extrapolate=True)
        logging.debug(cs_iv_curve)

        # Generate a fine grid for moneynesss
        min_moneyness = iv_curve['log_moneyness'].min()
        max_moneyness = iv_curve['log_moneyness'].max()
        moneyness_grid = np.linspace(min_moneyness, max_moneyness, 1000)

        # **Calculate SPD Using Volatility-Based Formula**
        spds, moneyness_support = [], []

        for moneyness in moneyness_grid:
            sigma = cs_iv_curve(float(moneyness))
            d2 = (-moneyness + (risk_free_rate - (sigma**2) / 2) * remaining_maturity) / (sigma * np.sqrt(remaining_maturity))
            spd_value = np.exp(-d2 ** 2 / 2) / (sigma * np.exp(moneyness) * latest_underlying_price * np.sqrt(2 * np.pi * remaining_maturity))
            spds.append(spd_value)
            moneyness_support.append(float(moneyness))

        spds_integral = np.trapezoid(spds, moneyness_support)
        print(f"SPD_Integral_RAW : {spds_integral}")
        spds /= spds_integral  # Normalize

        # Compute SPD Skewness
        mean_moneyness = np.trapezoid(spds * moneyness_support, moneyness_support)  # Expected value (mean)
        print(f"mean_moneyness: {mean_moneyness}")
        std_moneyness = np.sqrt(np.trapezoid((spds * (moneyness_support - mean_moneyness)**2), moneyness_support))  # Standard deviation
        spd_skewness = np.trapezoid((spds * (moneyness_support - mean_moneyness)**3), moneyness_support) / std_moneyness**3  # Skewness formula

        return moneyness_support, spds, iv_curve, spd_skewness

    except Exception as e:
        print(f"⚠️ Error processing SPD: {e}")
        return None, None, None, None


# **6. Update SPD & Volatility Curve Plots**
def update_spd_plot():
    selected_time = time_var.get()
    selected_expiration = exp_var.get()

    if not selected_time or not selected_expiration:
        messagebox.showerror("Error", "Please select both timestamp and expiration!")
        return

    unix_time = dict(timestamp_options)[selected_time]
    unix_exp = dict(expiration_options)[selected_expiration]

    df, latest_underlying_price, remaining_maturity = fetch_spd_data(unix_time, unix_exp)

    if df is None or df.empty:
        messagebox.showerror("Error", "No SPD data found for the selected time and expiration.")
        return

    moneyness_support, spds, iv_curve, spd_skewness = compute_spd(df, latest_underlying_price, remaining_maturity)

    if moneyness_support is None or spds is None:
        messagebox.showerror("Error", "SPD calculation failed.")
        logging.debug(moneyness_support)
        logging.debug(spds)
        return

    ax1.clear()
    ax1.plot(moneyness_support, spds, linestyle='-', marker='o', color='blue')
    ax1.set_title(f"SPD at {selected_time} (Exp: {selected_expiration})\nSPD Skewness: {spd_skewness:.4f}")
    ax1.set_xlabel("Log moneyness")
    ax1.set_ylabel("State Price Density")

    ax2.clear()
    ax2.plot(iv_curve['log_moneyness'], iv_curve['mark_iv'], linestyle='-', marker='o', color='red', label="IV")
    ax2.set_title("Volatility Curve")
    ax2.set_xlabel("Log moneyness")
    ax2.set_ylabel("Implied Volatility")
    ax2.legend()

    fig.tight_layout(pad=2.0)
    canvas.draw()

# **7. GUI Setup**
root = tk.Tk()
root.title("SPD & Volatility Tracker")
root.geometry("900x700")

timestamp_options = fetch_available_timestamps()
expiration_options = fetch_available_expirations()

time_var = tk.StringVar(value=timestamp_options[0][0])
exp_var = tk.StringVar(value=expiration_options[0][0])

ttk.Label(root, text="Select SPD Time:").pack()
ttk.Combobox(root, textvariable=time_var, values=[x[0] for x in timestamp_options]).pack()

ttk.Label(root, text="Select Expiration:").pack()
ttk.Combobox(root, textvariable=exp_var, values=[x[0] for x in expiration_options]).pack()

tk.Button(root, text="Show SPD", command=update_spd_plot).pack()

fig, (ax1, ax2) = plt.subplots(2, figsize=(8, 8))
canvas = FigureCanvasTkAgg(fig, master=root)
canvas.get_tk_widget().pack()

root.mainloop()