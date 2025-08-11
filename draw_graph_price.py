import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from statsmodels.tsa.stattools import adfuller, kpss
from datetime import datetime
import matplotlib

matplotlib.use("TkAgg")  # Use "QtAgg" for PyQt users


class plot_the_spread:
    def __init__(self) -> None:
        self.selected_expirations_raw: list = []
        self.selected_expirations = None
        self.expirations_pair: dict = {}
        self.get_user_expiration_dates()
        self.select_the_model()
        self.plot_spread()
    def select_the_model(self):
        if self.spread == True:
            self.plot_spread()
        else:
            if self.feasible == True:
                self.plot_data_with_std_dev_feasible()
            else:
                self.plot_data_with_std_dev()

    def get_user_expiration_dates(self):
        user_input = input("Enter expiration dates (format: DDMMMYY, separated by commas): ").strip().upper()
        user_input_way = input("Which way of skewness? Long or Short?").strip().upper()
        user_input_spread = input("Do you want to see the spread?: ").upper()

        self.spread_way = user_input_way

        if user_input_spread == "TRUE":
            self.spread = True
        else:
            self.spread = False
            user_input_feasible = input("Do you want to see the bid_ask price?: ").upper()
            if user_input_feasible == "TRUE":
                self.feasible = True
            elif user_input_feasible == "FALSE":
                self.feasible = False
            else:
                print("Please input bool.")

        expiration_dates = []

        for date_str in user_input.split(","):
            date_str = date_str.strip().upper()
            try:
                self.selected_expirations_raw.append(date_str)
                expiration_date = datetime.strptime(date_str, "%d%b%y")
                expiration_dates.append(int(expiration_date.timestamp()))
                self.expirations_pair[int(expiration_date.timestamp())] = date_str

            except ValueError:
                print(f"⚠️ Invalid date format: {date_str}. Please use DDMMMYY (e.g., 28MAR25).")

        self.selected_expirations = expiration_dates
        self.selected_expirations.sort()

    def fetch_data(self):

        sql = """
                SELECT timestamp, expiration_timestamp, option_type, bid_price, ask_price, bid_iv, ask_iv, log_moneyness, delta, theta
                FROM btc_options_raw
                WHERE timestamp >= UNIX_TIMESTAMP(NOW() - INTERVAL 0.5 DAY) * 1000
                ORDER BY timestamp DESC;
        """
        cursor.execute(sql)
        rows = cursor.fetchall()

        if not rows:
            return None

        df = pd.DataFrame(rows, columns=['timestamp', 'expiration_timestamp', 'option_type','bid_price', 'ask_price', 'bid_iv', 'ask_iv', 'log_moneyness', 'delta', 'theta'])
        df['timestamp'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(int(x)/1000).replace(microsecond=0))

        near_call_data = (
            df[(df['option_type'] == 'call') & (df['expiration_timestamp'] == self.selected_expirations[0])]
            .drop_duplicates('timestamp', keep='first').sort_values('timestamp'))
        near_call_data = near_call_data.set_index('timestamp')
        near_call_data = near_call_data.resample('20S').ffill()

        far_call_data = (
            df[(df['option_type'] == 'call') & (df['expiration_timestamp'] == self.selected_expirations[1])]
            .drop_duplicates('timestamp', keep='first').sort_values('timestamp'))
        far_call_data = far_call_data.set_index('timestamp')
        far_call_data = far_call_data.resample('20S').ffill()

        near_put_data = (
            df[(df['option_type'] == 'put') & (df['expiration_timestamp'] == self.selected_expirations[0])]
            .drop_duplicates('timestamp', keep='first').sort_values('timestamp')
        )
        near_put_data = near_put_data.set_index('timestamp')
        near_put_data = near_put_data.resample('20S').ffill()

        far_put_data = (
            df[(df['option_type'] == 'put') & (df['expiration_timestamp'] == self.selected_expirations[1])]
            .drop_duplicates('timestamp', keep='first').sort_values('timestamp')
        )
        far_put_data = far_put_data.set_index('timestamp')
        far_put_data = far_put_data.resample('20S').ffill()

        df['expiration_timestamp'] = df['expiration_timestamp'].apply(lambda x: datetime.fromtimestamp(int(x)))

        near_call_data['mid_price'] = (near_call_data['bid_price'] + near_call_data['ask_price']) / 2
        far_call_data['mid_price'] = (far_call_data['bid_price'] + far_call_data['ask_price']) / 2
        near_put_data['mid_price'] = (near_put_data['bid_price'] + near_put_data['ask_price']) / 2
        far_put_data['mid_price'] = (far_put_data['bid_price'] + far_put_data['ask_price']) / 2

        near_call_data['mid_iv'] = (near_call_data['bid_iv'] + near_call_data['ask_iv']) / 2
        far_call_data['mid_iv'] = (far_call_data['bid_iv'] + far_call_data['ask_iv']) / 2
        near_put_data['mid_iv'] = (near_put_data['bid_iv'] + near_put_data['ask_iv']) / 2
        far_put_data['mid_iv'] = (far_put_data['bid_iv'] + far_put_data['ask_iv']) / 2

        print(near_call_data)
        print(far_call_data)
        print(near_put_data)
        print(far_put_data)


        return near_call_data, far_call_data, near_put_data, far_put_data


    # **4. Generate Unique Colors for Each Expiration**
    def generate_colors(self, n):
        cmap = plt.get_cmap('tab10')  # Use distinct colors
        return [cmap(i) for i in range(n)]


    # **5. Plot SPD Skewness with Stationarity Test & Standard Deviation**
    def plot_data_with_std_dev(self):
        near_call, far_call, near_put, far_put = self.fetch_data()
        data = [near_call, far_call, near_put, far_put]
        colors = self.generate_colors(len(data))
        plt.figure(figsize=(12, 6))

        legend_handles = []

        for df, color in zip(data, colors):

            if df is None or df.empty:
                print("⚠️ No data found in MySQL.")
                return

            df = df.dropna()

            mean_price = df['mid_price'].mean()
            std_dev = df['mid_price'].std()

            modified_time_stamp = datetime.fromtimestamp(df['expiration_timestamp'].iloc[0])
            option_type = df['option_type'].iloc[0]



            # Plot SPD Skewness & store the handle for the legend
            curve, = plt.plot(df.index, df['mid_price'], linestyle='-', marker='o', markersize=2, color=color,
                              label=f"Exp: {modified_time_stamp.strftime('%d-%b-%Y')} {option_type}")
            legend_handles.append(curve)  # Add only skewness curves to the legend

            # Plot Mean Line (No label for legend)
            plt.axhline(mean_price, color=color, linestyle='--', alpha=0.5)

            # Plot ±3 Standard Deviation Bands (No label for legend)
            plt.fill_between(df.index,
                             mean_price - 2 * std_dev,
                             mean_price + 2 * std_dev,
                             color=color, alpha=0.2)

        plt.xlabel("Time")
        plt.ylabel("Mid Price")
        plt.title("Mid Price Over Time with Mean & ±2 Std Dev Bands")
        plt.legend(handles=legend_handles, loc="lower right")  # ✅ Only skewness curves in legend
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.show()


    def plot_data_with_std_dev_feasible(self):
        near_call, far_call, near_put, far_put = self.fetch_data()
        data = [near_call, far_call, near_put, far_put]

        if self.spread_way == 'LONG':
            data_using_ask = ["far_call", "near_put"]
            data_using_bid = ["near_call", "far_put"]
        elif self.spread_way == 'SHORT':
            data_using_bid = ["far_call", "near_put"]
            data_using_ask = ["near_call", "far_put"]
        else:
            print("Please check the way.")
            return

        titles = ["near_call", "far_call", "near_put", "far_put"]
        colors = self.generate_colors(4)
        plt.figure(figsize=(12, 6))

        legend_handles = []

        for df, color, title in zip(data, colors, titles):
            if df is None or df.empty:
                print("⚠️ No data found in MySQL.")
                return

            if title in data_using_bid:
                print("hello")
                df = df.dropna()

                mean_price = df['bid_price'].mean()
                std_dev = df['bid_price'].std()

                modified_time_stamp = datetime.fromtimestamp(df['expiration_timestamp'].iloc[0])
                option_type = df['option_type'].iloc[0]

                # Plot SPD Skewness & store the handle for the legend
                curve, = plt.plot(df.index, df['bid_price'], linestyle='-', marker='o', markersize=4, color=color,
                                  label=f"Exp: {modified_time_stamp.strftime('%d-%b-%Y')} {option_type}")
                legend_handles.append(curve)  # Add only skewness curves to the legend

                # Plot Mean Line (No label for legend)
                plt.axhline(mean_price, color=color, linestyle='--', alpha=0.5)

                # Plot ±3 Standard Deviation Bands (No label for legend)
                plt.fill_between(df.index,
                                 mean_price - 2 * std_dev,
                                 mean_price + 2 * std_dev,
                                 color=color, alpha=0.2)

            if title in data_using_ask:
                print("hello")
                df = df.dropna()

                mean_price = df['ask_price'].mean()
                std_dev = df['ask_price'].std()

                modified_time_stamp = datetime.fromtimestamp(df['expiration_timestamp'].iloc[0])
                option_type = df['option_type'].iloc[0]

                # Plot SPD Skewness & store the handle for the legend
                curve, = plt.plot(df.index, df['ask_price'], linestyle='-', marker='o', markersize=2, color=color,
                                  label=f"Exp: {modified_time_stamp.strftime('%d-%b-%Y')} {option_type}")
                legend_handles.append(curve)  # Add only skewness curves to the legend

                # Plot Mean Line (No label for legend)
                plt.axhline(mean_price, color=color, linestyle='--', alpha=0.5)

                # Plot ±3 Standard Deviation Bands (No label for legend)
                plt.fill_between(df.index,
                                 mean_price - 2 * std_dev,
                                 mean_price + 2 * std_dev,
                                 color=color, alpha=0.2)


        plt.xlabel("Time")
        plt.ylabel("Feasible Price")
        plt.title(f"Feasible Price Over Time with Mean & ±2 Std Dev Bands: {self.spread_way}")
        plt.legend(handles=legend_handles, loc="lower right")  # ✅ Only skewness curves in legend
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.show()

    def plot_spread(self):
        near_call, far_call, near_put, far_put = self.fetch_data()

        # Merge far_call and far_put
        merged_far = pd.merge(far_call, far_put, on='timestamp', suffixes=('_far_call', '_far_put'))

        # Merge near_put and near_call
        merged_near = pd.merge(near_put, near_call, on='timestamp', suffixes=('_near_put', '_near_call'))

        # Merge far and near results on timestamp
        final_df = pd.merge(merged_far, merged_near, on='timestamp')

        if self.spread_way == "LONG":
            final_df['far_spread'] = - final_df['ask_price_far_call'] + final_df['bid_price_far_put']
            final_df['near_spread'] = - final_df['ask_price_near_put'] + final_df['bid_price_near_call']
            final_df['RR_spread'] = final_df['far_spread'] + final_df['near_spread']
        elif self.spread_way == "SHORT":
            final_df['far_spread'] = final_df['bid_price_far_call'] - final_df['ask_price_far_put']
            final_df['near_spread'] = final_df['bid_price_near_put'] - final_df['ask_price_near_call']
            final_df['RR_spread'] = final_df['far_spread'] + final_df['near_spread']
        else:
            print("Please check the way.")
            return

        final_df = final_df.dropna()

        if final_df is None or final_df.empty:
            print("⚠️ No appropriate data found in MySQL.")
        else:
            legend_handles = []
            colors = self.generate_colors(3)
            plt.figure(figsize=(12, 6))
            columns = ['far_spread', 'near_spread', 'RR_spread']

            for column, color in zip(columns, colors):
                mean_price = final_df[f'{column}'].mean()
                std_dev = final_df[f'{column}'].std()

                # Plot SPD Skewness & store the handle for the legend
                curve, = plt.plot(final_df.index, final_df[f'{column}'], linestyle='-', marker='o', markersize=2,
                                  color=color,
                                  label=f"{column}")
                legend_handles.append(curve)  # Add only skewness curves to the legend

                # Plot Mean Line (No label for legend)
                plt.axhline(mean_price, color=color, linestyle='--', alpha=0.5)

                # Plot ±3 Standard Deviation Bands (No label for legend)
                plt.fill_between(final_df.index,
                                 mean_price - 2 * std_dev,
                                 mean_price + 2 * std_dev,
                                 color=color, alpha=0.2)

            plt.xlabel("Time")
            plt.ylabel("Spread")
            plt.title(f"Spread Over Time with Mean & ±2 Std Dev Bands: {self.spread_way}")
            plt.legend(handles=legend_handles, loc="lower right")  # ✅ Only skewness curves in legend
            plt.grid(True)
            plt.xticks(rotation=45)
            plt.show()

# **8. Run Static or Real-Time Graph**
if __name__ == "__main__":
    # **1. Connect to MySQL**
    conn = mysql.connector.connect(
        host="localhost",
        user="root",  # Change if needed
        password="1234",  # Change if needed
        database="btc_options_db"
    )
    cursor = conn.cursor()

    test = plot_the_spread()