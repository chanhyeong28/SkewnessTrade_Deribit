import asyncio
import sys
import json
import logging
from typing import Dict
from datetime import datetime, timedelta
import websockets
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
import secrets  # Secure random nonce generator
import base64
import re
import mysql.connector
import numpy as np
import pandas as pd
from scipy.interpolate import CubicSpline
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes


class WebSocketClient:
    def __init__(self, ws_connection_url, client_id, timestamp, encoded_signature, nonce, data) -> None:

        # Instance Variables
        self.ws_connection_url: str = ws_connection_url
        self.client_id: str = client_id
        self.timestamp = timestamp
        self.encoded_signature = encoded_signature
        self.nonce = nonce
        self.data = data
        self.websocket_client: websockets.WebSocketClientProtocol = None
        self.access_token = None
        self.refresh_token = None
        self.refresh_token_expiry_time = None
        self.latest_underlying_prices: dict = {}
        self.otm_call: dict = {}
        self.otm_put: dict = {}
        self.perpetual_expirations_raw: list = []
        self.selected_expirations_raw: list = []
        self.selected_expirations = None
        self.expirations_pair: dict = {}
        self.selected_expirations_subscribe: list = []
        self.portfolio_status = None
        self.portfolio_position = None
        # self.get_user_expiration_dates()
        # self.generate_subscribe()
        self.loop = asyncio.new_event_loop()

    def start(self):
        print("Hi")
        self.loop.run_until_complete(self.ws_manager())

    def extract_strike_price_type_expiration(self, instr_name):
        match = re.search(r'-(\d{2}[A-Z]{3}\d{2})-(\d+)-([CP])', instr_name)
        if match:
            date_str = match.group(1)
            strike_price = float(match.group(2))
            option_type = "call" if match.group(3) == "C" else "put"

            try:
                expiration_date = datetime.strptime(date_str, "%d%b%y")
                expiration_timestamp = int(expiration_date.timestamp())
            except ValueError:
                expiration_timestamp = None

            return strike_price, option_type, expiration_timestamp, date_str

        return None, None, None, None

    def generate_subscribe(self):
        for date in self.selected_expirations_raw:
            self.selected_expirations_subscribe.append(f"ticker.BTC-{date}.100ms")
            self.latest_underlying_prices[f"BTC-{date}"] = False
        self.selected_expirations_subscribe.append("markprice.options.btc_usd")
        self.selected_expirations_subscribe.append("ticker.BTC-PERPETUAL.100ms")

    async def ws_manager(self) -> None:
        async with (websockets.connect(self.ws_connection_url, ping_interval=None, compression=None,close_timeout=60)
                    as self.websocket_client):

            # Authenticate WebSocket Connection
            await self.ws_auth()

            # Establish Heartbeat
            await self.establish_heartbeat()

            self.loop.create_task(self.ws_refresh_auth())

            await self.ws_subscribe(operation='subscribe', ws_channel=self.selected_expirations_subscribe)

            while self.websocket_client.state == websockets.protocol.State.OPEN:
                message: bytes = await self.websocket_client.recv()
                message: Dict = json.loads(message)

                if 'id' in list(message):
                    if message['id'] == 9929:
                        if self.refresh_token is None:
                            logging.info('Successfully authenticated WebSocket Connection')
                            logging.info(message)
                        else:
                            logging.info('Successfully refreshed the authentication of the WebSocket Connection')
                        self.access_token = message['result']['access_token']
                        self.refresh_token = message['result']['refresh_token']

                        # Refresh Authentication well before the required datetime
                        if message['testnet']:
                            expires_in: int = 300
                        else:
                            expires_in: int = message['result']['expires_in'] - 240

                        self.refresh_token_expiry_time = datetime.now() + timedelta(seconds=expires_in)

                    elif message['id'] == 8212:
                        # Avoid logging Heartbeat messages
                        continue

                    elif message['id'] == 1005:
                        logging.info(f"Result of Simulation: {message}")
                        if message['result'] is not None:
                            self.portfolio_status = message


                    elif message['id'] == 1006:
                        logging.info(f"Position: {message}")
                        if message['result'] is not None:
                            self.portfolio_position = message["result"]

                    elif message['id'] == 1001:
                        logging.info(f"Result of executes: {message}")

                elif 'method' in list(message):
                    # Respond to Heartbeat Message
                    if message['method'] == 'heartbeat':
                        await self.heartbeat_response()

                    elif message['method'] == 'subscription':
                        logging.debug(f"Market Data Received: {message}")
                        channel = message["params"]["channel"]

                        for date in self.selected_expirations_raw:
                            # Updating BTC price
                            if channel == f"ticker.BTC-{date}.100ms":
                                self.latest_underlying_prices[f"BTC-{date}"] = message["params"]["data"].get("mark_price", None)
                                print("🔹 Updated BTC Future Price For {0}: {1}".format(date, self.latest_underlying_prices[f"BTC-{date}"]))

                            if channel == "ticker.BTC-PERPETUAL.100ms":
                                if date in self.perpetual_expirations_raw:
                                    self.latest_underlying_prices[f"BTC-{date}"] = message["params"]["data"].get("mark_price", None)
                                    print("🔹 Updated BTC Future Price For {0}: {1}".format(date,
                                                                                           self.latest_underlying_prices[
                                                                                               f"BTC-{date}"]))
                            # Updating Options
                            if bool(re.match(fr"^ticker\.BTC-{date}-(\d+)-([CP])\.100ms", channel)):
                                option_data = message["params"]["data"]
                                logging.debug(option_data)

                                timestamp = option_data.get('timestamp', None)
                                instrument_name = option_data.get('instrument_name', None)
                                bid_price = option_data.get('best_bid_price', None)
                                ask_price = option_data.get('best_ask_price', None)
                                bid_iv = option_data.get('bid_iv', None)
                                ask_iv = option_data.get('ask_iv', None)
                                delta = option_data['greeks'].get('delta', None)
                                vega = option_data['greeks'].get('vega', None)
                                theta = option_data['greeks'].get('theta', None)

                                strike_price, option_type, expiration_timestamp, date_str = self.extract_strike_price_type_expiration(instrument_name)

                                if strike_price is None or expiration_timestamp is None:
                                    continue

                                if option_type == "call" and strike_price < self.latest_underlying_prices[f"BTC-{date_str}"]:
                                    continue
                                if option_type == "put" and strike_price > self.latest_underlying_prices[f"BTC-{date_str}"]:
                                    continue

                                log_moneyness = np.log(strike_price / self.latest_underlying_prices[f"BTC-{date_str}"])

                                # Storing through SQL
                                sql = """INSERT INTO btc_options_raw (timestamp, instrument_name, expiration_timestamp, option_type,
                                                                        bid_price, ask_price, bid_iv, ask_iv, 
                                                                        underlying_price, strike_price, log_moneyness,
                                                                        delta, vega, theta) 
                                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                values = (timestamp, instrument_name, expiration_timestamp, option_type,
                                          bid_price, ask_price, bid_iv, ask_iv,
                                          self.latest_underlying_prices[f"BTC-{date_str}"],
                                          strike_price, log_moneyness,
                                          delta, vega, theta)
                                cursor.execute(sql, values)
                                conn.commit()

                                print("✅ Tick data inserted at {0} for {1}".format(datetime.now(),f"BTC-{date_str}"))

                            # Updating Option
                            if channel == 'markprice.options.btc_usd':
                                curve_data = message["params"]["data"]

                                for element in curve_data:
                                    instrument_name = element.get('instrument_name', None)
                                    timestamp = element.get('timestamp', None)
                                    mark_price = element.get('mark_price', None)
                                    mark_iv = element.get('iv', None)

                                    if not instrument_name or not timestamp or not mark_price or not mark_iv:
                                        continue

                                    strike_price, option_type, expiration_timestamp, date_str = self.extract_strike_price_type_expiration(
                                        instrument_name)

                                    if strike_price is None or expiration_timestamp is None:
                                        continue

                                    # **Filter Only Selected Expiration Dates**
                                    if expiration_timestamp not in self.selected_expirations:
                                        continue

                                    if self.latest_underlying_prices[f"BTC-{date_str}"] is None:
                                        continue

                                    if option_type == "call" and strike_price < self.latest_underlying_prices[
                                        f"BTC-{date_str}"]:
                                        continue
                                    if option_type == "put" and strike_price > self.latest_underlying_prices[
                                        f"BTC-{date_str}"]:
                                        continue

                                    log_moneyness = np.log(
                                        strike_price / self.latest_underlying_prices[f"BTC-{date_str}"])

                                    sql = """INSERT INTO btc_options_tick (timestamp, instrument_name, underlying_price, strike_price, 
                                                                               mid_price, mark_iv, expiration_timestamp, option_type, log_moneyness) 
                                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                    values = (
                                    timestamp, instrument_name, self.latest_underlying_prices[f"BTC-{date_str}"],
                                    strike_price,
                                    mark_price, mark_iv, expiration_timestamp, option_type, log_moneyness)
                                    cursor.execute(sql, values)
                                    conn.commit()

                                    print(
                                        "✅ Curve data inserted at {0} for {1}".format(datetime.now(), f"BTC-{date_str}"))

            else:
                logging.info('WebSocket connection has broken.')
                sys.exit(1)

    async def establish_heartbeat(self) -> None:
        """
        Requests DBT's `public/set_heartbeat` to
        establish a heartbeat connection.
        """
        msg: Dict = {
                    "jsonrpc": "2.0",
                    "id": 9098,
                    "method": "public/set_heartbeat",
                    "params": {
                              "interval": 10
                               }
                    }

        await self.websocket_client.send(
            json.dumps(
                msg
                )
                )


    async def heartbeat_response(self) -> None:
        """
        Sends the required WebSocket response to
        the Deribit API Heartbeat message.
        """
        msg: Dict = {
                    "jsonrpc": "2.0",
                    "id": 8212,
                    "method": "public/test",
                    "params": {}
                    }

        await self.websocket_client.send(
            json.dumps(
                msg
                )
                )

    async def ws_auth(self):
        """
        Authenticate WebSocket Connection.
        """
        msg = {
            "jsonrpc": "2.0",
            "id": 9929,
            "method": "public/auth",
            "params": {
                "grant_type": "client_signature",
                "client_id": self.client_id,
                "timestamp": self.timestamp,
                "signature": self.encoded_signature,
                "nonce": self.nonce,  # Secure random nonce
                "data": self.data
            }
        }
        await self.websocket_client.send(json.dumps(msg))
        logging.info(f"Request for auth: {msg}")


    async def ws_refresh_auth(self) -> None:
        """
        Requests DBT's `public/auth` to refresh
        the WebSocket Connection's authentication.
        """
        while True:
            if self.refresh_token_expiry_time is not None:
                if datetime.now() > self.refresh_token_expiry_time:
                    msg: Dict = {
                                "jsonrpc": "2.0",
                                "id": 9929,
                                "method": "public/auth",
                                "params": {
                                          "grant_type": "refresh_token",
                                          "refresh_token": self.refresh_token
                                            }
                                }

                    await self.websocket_client.send(
                        json.dumps(
                            msg
                            )
                            )

            await asyncio.sleep(5)

    async def ws_subscribe(self, operation: str, ws_channel: list) -> None:
        """
        Requests `public/subscribe` or `public/unsubscribe`
        to DBT's API for the specific WebSocket Channel.
        """
        await asyncio.sleep(1)

        msg: Dict = {
                    "jsonrpc": "2.0",
                    "method": f"private/{operation}",
                    "id": 42,
                    "params": {
                        "channels": ws_channel
                        }
                    }

        await self.websocket_client.send(
            json.dumps(
                msg
                )
            )
        logging.info(f"Request for subscribe: {msg}")

        await asyncio.sleep(5)

    async def place_order_buy(self, instrument_name: str, amount: float, price: float = None, time_in_force: str = "fill_or_kill",
                              reduce_only: str = 'false', advanced: str = None , order_type: str = "market", label: str = None, post_only: bool = False):
        """
        Place a buy or sell order.
        direction: "buy" or "sell"
        order_type: "limit" or "market"
        """
        msg = {
            "jsonrpc": "2.0",
            "id": 1001,
            "method": "private/buy",
            "params": {
                "instrument_name": instrument_name,
                "amount": amount,
                "type": order_type,
                "price": price,
                "post_only": post_only,
                "reduce_only": reduce_only,
            }
        }
        if label:
            msg["params"]["label"] = label

        await self.websocket_client.send(json.dumps(msg))
        logging.info(f"Buy order sent: {msg}")

    async def place_order_sell(self, instrument_name: str, amount: float, price: float = None, time_in_force: str = "fill_or_kill",
                              reduce_only: str = 'false', advanced: str = None, order_type: str = "market", label: str = None, post_only: bool = False):
        """
        Place a buy or sell order.
        direction: "buy" or "sell"
        order_type: "limit" or "market"
        """
        msg = {
            "jsonrpc": "2.0",
            "id": 1001,
            "method": "private/sell",
            "params": {
                "instrument_name": instrument_name,
                "amount": amount,
                "type": order_type,
                "price": price,
                "post_only": post_only,
                "reduce_only": reduce_only,
            }
        }
        if label:
            msg["params"]["label"] = label

        await self.websocket_client.send(json.dumps(msg))
        logging.info(f"Sell order sent: {msg}")

    async def cancel_order(self, order_id: str):
        """
        Cancel a single order.
        """
        msg = {
            "jsonrpc": "2.0",
            "id": 1002,
            "method": "private/cancel",
            "params": {
                "order_id": order_id
            }
        }
        await self.websocket_client.send(json.dumps(msg))
        logging.info(f"Cancel order sent: {msg}")

    async def edit_order(self, order_id: str, price: float = None, amount: float = None):
        """
        Edit an existing order by changing its price or amount.
        """
        params = {
            "order_id": order_id
        }
        if price is not None:
            params["price"] = price
        if amount is not None:
            params["amount"] = amount

        msg = {
            "jsonrpc": "2.0",
            "id": 1003,
            "method": "private/edit",
            "params": params
        }
        await self.websocket_client.send(json.dumps(msg))
        logging.info(f"Edit order sent: {msg}")

    async def create_combo(self, order_list: list):
        """
        Create a combo order (multi-leg strategy).
        order_list: list of dicts with { "instrument_name", "amount", "price", "direction" }
        """
        combo = []
        for leg in order_list:
            combo.append({
                "instrument_name": leg["instrument_name"],
                "amount": leg["amount"],
                "price": leg["price"],
                "direction": leg["direction"]
            })

        msg = {
            "jsonrpc": "2.0",
            "id": 1004,
            "method": "private/create_combo_order",
            "params": {
                "combo_order": combo
            }
        }

        await self.websocket_client.send(json.dumps(msg))
        logging.info(f"Combo order sent: {msg}")

    async def simulate_portfolio(self, simulated_positions: dict):
        """
        Simulate portfolio with hypothetical positions and return margin/liquidation estimates.

        Args:
            simulated_positions (dict): Dictionary like
                {
                    "BTC-PERPETUAL": 1000.0,
                    "BTC-5JUL21-40000-C": 10.0
                }
        """
        msg = {
            "jsonrpc": "2.0",
            "id": 1005,
            "method": "private/simulate_portfolio",
            "params": {
                "currency": "BTC",
                "add_positions": True,
                "simulated_positions": simulated_positions
            }
        }

        await self.websocket_client.send(json.dumps(msg))
        logging.info(f"🔍 Sent simulate_portfolio request with positions: {simulated_positions}")

    async def get_positions(self, kind: str = "option", currency: str = "BTC"):
        """
        Retrieve current open positions filtered by instrument kind.

        Args:
            kind (str): "future", "option", or leave as None to get all.
        """
        params = {
            "currency": "BTC"
        }
        if kind:
            params["kind"] = kind

        msg = {
            "jsonrpc": "2.0",
            "id": 1006,
            "method": "private/get_positions",
            "params": params
        }
        await self.websocket_client.send(json.dumps(msg))
        logging.info(f"📦 get_positions request sent (kind={kind})")


class Strategy_RR(WebSocketClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.enabled = False
        self.strike_prices: dict = {}
        self.spread_lower_bound = None
        self.spread_upper_bound = None
        self.latest_rr_spread = None
        self.latest_rr_spread_price = None
        self.pre_margin_check_long = False
        self.pre_margin_check_short = False
        self.spread_way = "SHORT"
        self.get_user_expiration_dates()
        self.generate_subscribe()
        self.loop.create_task(self.update_subscribe())
        self.loop.create_task(self.compute_spd_skewness())
        self.loop.create_task(self.risk_manager())
        self.loop.create_task(self.should_execute())
        self.loop.create_task(self.update_position())

    def get_user_expiration_dates(self):
        user_input = input("Enter expiration dates (format: DDMMMYY, separated by commas): ").strip()
        user_input_2 = input("Please input any expiration dates using PERPETUAL price (format: DDMMMYY, separated by commas): ").strip()
        user_input_3 = input("Please input way. SHORT? LONG?: ").upper()
        user_input_4 = input("Execute enabled?: ").upper()
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

        for date_str in user_input_2.split(","):
            date_str = date_str.strip().upper()
            try:
                self.perpetual_expirations_raw.append(date_str)
            except ValueError:
                print(f"⚠️ Invalid date format: {date_str}. Please use DDMMMYY (e.g., 28MAR25).")

        expiration_dates.sort()
        self.selected_expirations = expiration_dates
        self.spread_way = user_input_3

        if user_input_4 == "TRUE":
            self.enabled = True
        else:
            self.enabled = False

    async def update_position(self):
        await asyncio.sleep(10)

        while True:
            await self.get_positions()

            await asyncio.sleep(60)  # Sleep to avoid tight loop

    async def update_subscribe(self):
        # Adopting strike prices whose log moneyness is closest to 0.1 and -0.1, respectively
        await asyncio.sleep(20)

        while True:
            pre_selected_expirations_subscribe = self.selected_expirations_subscribe
            self.selected_expirations_subscribe: list = []

            if len(self.latest_underlying_prices) != len(self.selected_expirations_raw):
                print(f"⚠️ Underlying prices are needed.Please wait...")
                await asyncio.sleep(20)
                continue

            if self.strike_prices == {}:
                print(f"⚠️ Strike prices are needed. Please wait...")
                await asyncio.sleep(20)
                continue

            for date in self.selected_expirations_raw:
                self.otm_call[f"BTC-{date}"]: list = []
                closest_call = min(self.strike_prices[date], key=lambda x: abs(x - self.latest_underlying_prices[f"BTC-{date}"]*np.exp(0.1)))
                closest_call = int(closest_call)
                self.otm_call[f"BTC-{date}"].append(closest_call)

                self.otm_put[f"BTC-{date}"]: list = []
                closest_put = min(self.strike_prices[date], key=lambda x: abs(x - (self.latest_underlying_prices[f"BTC-{date}"])**2/closest_call))
                closest_put = int(closest_put)
                self.otm_put[f"BTC-{date}"].append(closest_put)

                for level in self.otm_call[f"BTC-{date}"]:
                    self.selected_expirations_subscribe.append(
                        "ticker.BTC-{0}-{1}-C.100ms".format(date, level))

                for level in self.otm_put[f"BTC-{date}"]:
                    self.selected_expirations_subscribe.append(
                        "ticker.BTC-{0}-{1}-P.100ms".format(date, level))

            await self.ws_subscribe(operation='unsubscribe', ws_channel= pre_selected_expirations_subscribe)
            await self.ws_subscribe(operation='subscribe', ws_channel=self.selected_expirations_subscribe)
            print(f"Subscriptions are updated. {self.selected_expirations_subscribe}")
            await asyncio.sleep(600)

    async def compute_spd_skewness(self):
        risk_free_rate = 0.043  # Example risk-free rate (4.3%)

        while True:
            await asyncio.sleep(60)

            for exp_ts in self.selected_expirations:
                # btc_option_tick using 13digit unix time
                sql = """
                    SELECT timestamp, strike_price, mark_iv, option_type, underlying_price
                    FROM btc_options_tick 
                    WHERE expiration_timestamp = %s
                    and timestamp >= UNIX_TIMESTAMP(NOW()-INTERVAL 1 DAY) * 1000
                    ORDER BY timestamp DESC 
                    ;
                """
                cursor.execute(sql, (exp_ts,))
                rows = cursor.fetchall()
                logging.debug(rows)

                if not rows:
                    continue

                exp_data = pd.DataFrame(rows, columns=['timestamp', 'strike_price', 'mark_iv', 'option_type', 'underlying_price'])

                local_latest_underlying_price = exp_data['underlying_price'].iloc[0] if not exp_data.empty else None
                current_time = exp_data['timestamp'].iloc[0] if not exp_data.empty else None

                formatted_date = self.expirations_pair[exp_ts]

                if exp_data.empty:
                    continue

                # **Ensure Unique Strike Prices and Sort**
                call_data = exp_data[exp_data['option_type'] == 'call'].drop_duplicates('strike_price', keep='first').sort_values(
                    'strike_price')
                call_data['log_moneyness'] = np.log(call_data["strike_price"] / local_latest_underlying_price)
                put_data = exp_data[exp_data['option_type'] == 'put'].drop_duplicates('strike_price', keep='first').sort_values(
                    'strike_price')
                put_data['log_moneyness'] = np.log(put_data["strike_price"] / local_latest_underlying_price)

                # **Check for Minimum Data Requirement**
                if len(call_data) < 4 or len(put_data) < 4:
                    print(f"⚠️ Not enough unique data for expiration {formatted_date}. Skipping...")
                    continue

                try:
                    # **Get Remaining Maturity (T-t) in Years**
                    # current_time = datetime.now().timestamp()
                    remaining_maturity = (int(exp_ts) - (int(current_time)/1000)) / (
                                365 * 24 * 60 * 60)  # Convert seconds to years

                    if remaining_maturity <= 0:
                        print(f"⚠️ Expiry {formatted_date} already passed. Skipping...")
                        continue

                    # **Interpolate Volatility Curve**
                    iv_curve = pd.concat([put_data, call_data]).sort_values('log_moneyness')
                    self.strike_prices[self.expirations_pair[exp_ts]] = sorted(iv_curve["strike_price"].unique())
                    iv_curve = iv_curve.groupby('log_moneyness', as_index=False)['mark_iv'].mean()
                    print(f'amount of data : {len(iv_curve)}')
                    logging.debug(iv_curve)
                    logging.debug(remaining_maturity)
                    cs_iv_curve = CubicSpline(iv_curve['log_moneyness'].astype(float), iv_curve['mark_iv'].astype(float),
                                              extrapolate=True)
                    atm_slope = cs_iv_curve.derivative()(0)
                    print("ATM Slope:", float(atm_slope))

                    # **Save SPD Skewness to MySQL with a timestamp**
                    sql = """
                        INSERT IGNORE INTO btc_iv_spd_skewness (timestamp, expiration_timestamp, atm_slope) 
                        VALUES (%s, %s, %s)
                    """
                    values = (int(current_time), exp_ts, float(atm_slope)) # current_time already in ms
                    cursor.execute(sql, values)
                    conn.commit()

                    print(f"📊 SPD Skewness for Expiry {formatted_date} at {datetime.now()} : {float(atm_slope)}")

                except Exception as e:
                    print(f"⚠️ Error processing expiration {formatted_date}: {e}")
                    continue

    def fetch_data(self):
        # Fetching data in past 12 hours
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
        near_call_data = near_call_data.resample('20s').ffill()

        far_call_data = (
            df[(df['option_type'] == 'call') & (df['expiration_timestamp'] == self.selected_expirations[1])]
            .drop_duplicates('timestamp', keep='first').sort_values('timestamp'))
        far_call_data = far_call_data.set_index('timestamp')
        far_call_data = far_call_data.resample('20s').ffill()

        near_put_data = (
            df[(df['option_type'] == 'put') & (df['expiration_timestamp'] == self.selected_expirations[0])]
            .drop_duplicates('timestamp', keep='first').sort_values('timestamp')
        )
        near_put_data = near_put_data.set_index('timestamp')
        near_put_data = near_put_data.resample('20s').ffill()

        far_put_data = (
            df[(df['option_type'] == 'put') & (df['expiration_timestamp'] == self.selected_expirations[1])]
            .drop_duplicates('timestamp', keep='first').sort_values('timestamp')
        )
        far_put_data = far_put_data.set_index('timestamp')
        far_put_data = far_put_data.resample('20s').ffill()

        df['expiration_timestamp'] = df['expiration_timestamp'].apply(lambda x: datetime.fromtimestamp(int(x)))

        near_call_data['mid_price'] = (near_call_data['bid_price'] + near_call_data['ask_price']) / 2
        far_call_data['mid_price'] = (far_call_data['bid_price'] + far_call_data['ask_price']) / 2
        near_put_data['mid_price'] = (near_put_data['bid_price'] + near_put_data['ask_price']) / 2
        far_put_data['mid_price'] = (far_put_data['bid_price'] + far_put_data['ask_price']) / 2

        near_call_data['mid_iv'] = (near_call_data['bid_iv'] + near_call_data['ask_iv']) / 2
        far_call_data['mid_iv'] = (far_call_data['bid_iv'] + far_call_data['ask_iv']) / 2
        near_put_data['mid_iv'] = (near_put_data['bid_iv'] + near_put_data['ask_iv']) / 2
        far_put_data['mid_iv'] = (far_put_data['bid_iv'] + far_put_data['ask_iv']) / 2

        return near_call_data, far_call_data, near_put_data, far_put_data

    async def risk_manager(self):
        while True:
            await asyncio.sleep(60)
            try:
                near_call, far_call, near_put, far_put = self.fetch_data()

                # Merge far_call and far_put
                merged_far = pd.merge(far_call, far_put, on='timestamp', suffixes=('_far_call', '_far_put'))

                # Merge near_put and near_call
                merged_near = pd.merge(near_put, near_call, on='timestamp', suffixes=('_near_put', '_near_call'))

                # Merge far and near results on timestamp
                final_df = pd.merge(merged_far, merged_near, on='timestamp')

                if self.spread_way == "LONG":
                    final_df['far_spread'] = - final_df['ask_iv_far_call'] + final_df['bid_iv_far_put']
                    final_df['near_spread'] = - final_df['ask_iv_near_put'] + final_df['bid_iv_near_call']
                    final_df['RR_spread'] = final_df['far_spread'] + final_df['near_spread']
                    final_df['far_spread_price'] = - final_df['ask_price_far_call'] + final_df['bid_price_far_put']
                    final_df['near_spread_price'] = - final_df['ask_price_near_put'] + final_df['bid_price_near_call']
                    final_df['RR_spread_price'] = final_df['far_spread_price'] + final_df['near_spread_price']
                elif self.spread_way == "SHORT":
                    final_df['far_spread'] = final_df['bid_iv_far_call'] - final_df['ask_iv_far_put']
                    final_df['near_spread'] = final_df['bid_iv_near_put'] - final_df['ask_iv_near_call']
                    final_df['RR_spread'] = final_df['far_spread'] + final_df['near_spread']
                    final_df['far_spread_price'] = final_df['bid_price_far_call'] - final_df['ask_price_far_put']
                    final_df['near_spread_price'] = final_df['bid_price_near_put'] - final_df['ask_price_near_call']
                    final_df['RR_spread_price'] = final_df['far_spread_price'] + final_df['near_spread_price']
                else:
                    print("Please check the way.")
                    return

                final_df = final_df.dropna()
                final_df = final_df.sort_values("timestamp")
                latest_row = final_df.sort_values("timestamp").iloc[-1]
                self.latest_rr_spread = latest_row["RR_spread"]
                self.latest_rr_spread_price = latest_row["RR_spread_price"]
                print(f'latest_rr_spread: {self.latest_rr_spread}')
                print(f'latest_rr_spread_price: {self.latest_rr_spread_price}')

                # Calculate mean and standard deviation (You can use it as a threshold)
                mean_spread = final_df['RR_spread'].mean()
                std_dev = final_df['RR_spread'].std()
                self.spread_lower_bound = mean_spread - 2 * std_dev
                self.spread_upper_bound = mean_spread + 2 * std_dev

                if (self.otm_call == {}) or (self.otm_put == {}):
                    print("⚠️ Simulation: Please wait until getting the strike prices")
                    continue

                if self.spread_way == "SHORT":
                    await self.simulate_portfolio({
                                                   "BTC-{0}-{1}-C".format(
                                                       self.expirations_pair[self.selected_expirations[1]],
                                                       self.otm_call[
                                                           f"BTC-{self.expirations_pair[self.selected_expirations[1]]}"][0]): -0.1,
                                                   "BTC-{0}-{1}-P".format(
                                                       self.expirations_pair[self.selected_expirations[1]],
                                                       self.otm_put[
                                                           f"BTC-{self.expirations_pair[self.selected_expirations[1]]}"][0]): 0.1,
                                                   "BTC-{0}-{1}-C".format(
                                                       self.expirations_pair[self.selected_expirations[0]],
                                                       self.otm_call[
                                                           f"BTC-{self.expirations_pair[self.selected_expirations[0]]}"][0]): 0.1,
                                                   "BTC-{0}-{1}-P".format(
                                                       self.expirations_pair[self.selected_expirations[0]],
                                                       self.otm_put[
                                                           f"BTC-{self.expirations_pair[self.selected_expirations[0]]}"][0]): -0.1,
                                                   })

                    await asyncio.sleep(5)
                    if self.portfolio_status == None:
                        self.pre_margin_check_short = False
                    elif self.portfolio_status['result']['equity'] < self.portfolio_status['result'][
                        'projected_maintenance_margin'] * 1.2:
                        self.pre_margin_check_short = False
                    elif self.portfolio_status['result']['margin_balance'] < self.portfolio_status['result'][
                        'projected_initial_margin']:
                        self.pre_margin_check_short = False
                    else:
                        self.pre_margin_check_short = True
                    print(f"pre_margin_check: {self.pre_margin_check_short}")


                elif self.spread_way == "LONG":
                    await self.simulate_portfolio({
                        "BTC-{0}-{1}-C".format(
                            self.expirations_pair[self.selected_expirations[1]],
                            self.otm_call[
                                f"BTC-{self.expirations_pair[self.selected_expirations[1]]}"][0]): 0.1,
                        "BTC-{0}-{1}-P".format(
                            self.expirations_pair[self.selected_expirations[1]],
                            self.otm_put[
                                f"BTC-{self.expirations_pair[self.selected_expirations[1]]}"][0]): -0.1,
                        "BTC-{0}-{1}-C".format(
                            self.expirations_pair[self.selected_expirations[0]],
                            self.otm_call[
                                f"BTC-{self.expirations_pair[self.selected_expirations[0]]}"][0]): -0.1,
                        "BTC-{0}-{1}-P".format(
                            self.expirations_pair[self.selected_expirations[0]],
                            self.otm_put[
                                f"BTC-{self.expirations_pair[self.selected_expirations[0]]}"][0]): 0.1,
                    })

                    await asyncio.sleep(5)
                    if self.portfolio_status == None:
                        self.pre_margin_check_long = False
                    elif self.portfolio_status['result']['equity'] < self.portfolio_status['result'][
                        'projected_maintenance_margin'] * 1.2:
                        self.pre_margin_check_long = False
                    elif self.portfolio_status['result']['margin_balance'] < self.portfolio_status['result'][
                        'projected_initial_margin']:
                        self.pre_margin_check_long = False
                    else:
                        self.pre_margin_check_long = True
                    print(f"pre_margin_check: {self.pre_margin_check_long}")

                print(f"enabled? :{self.enabled}")

            except Exception as e:
                print(f"⚠️ Error processing while simulation: {e}")
                continue

    async def should_execute(self):

        while True:
            await asyncio.sleep(60)
            try:
                if self.enabled:
                    # To trade 0.1 amounts of RR when spreads for both IV and price are positive
                    if (self.latest_rr_spread > 0) & (self.latest_rr_spread_price > 0):
                        logging.info(f"pre_margin_check_long = {self.pre_margin_check_long}")
                        logging.info(f"pre_margin_check_short = {self.pre_margin_check_short}")

                        if (self.spread_way == "SHORT") & (self.pre_margin_check_short == True):
                            await self.place_order_sell(instrument_name ="BTC-{0}-{1}-C".format(
                                    self.expirations_pair[self.selected_expirations[1]],
                                    self.otm_call[
                                        f"BTC-{self.expirations_pair[self.selected_expirations[1]]}"][0]), amount=0.1)
                            await self.place_order_buy(instrument_name="BTC-{0}-{1}-P".format(
                                    self.expirations_pair[self.selected_expirations[1]],
                                    self.otm_put[
                                        f"BTC-{self.expirations_pair[self.selected_expirations[1]]}"][0]), amount=0.1)
                            await self.place_order_buy(instrument_name="BTC-{0}-{1}-C".format(
                                    self.expirations_pair[self.selected_expirations[0]],
                                    self.otm_call[
                                        f"BTC-{self.expirations_pair[self.selected_expirations[0]]}"][0]), amount=0.1)
                            await self.place_order_sell(instrument_name="BTC-{0}-{1}-P".format(
                                    self.expirations_pair[self.selected_expirations[0]],
                                    self.otm_put[
                                        f"BTC-{self.expirations_pair[self.selected_expirations[0]]}"][0]), amount=0.1)
                            print("SHORT EXECUTE!!!")
                            self.enabled = False
                            continue

                        if (self.spread_way == "LONG") & (self.pre_margin_check_long == True):
                            await self.place_order_buy(instrument_name="BTC-{0}-{1}-C".format(
                                self.expirations_pair[self.selected_expirations[1]],
                                self.otm_call[
                                    f"BTC-{self.expirations_pair[self.selected_expirations[1]]}"][0]), amount=0.1)
                            await self.place_order_sell(instrument_name="BTC-{0}-{1}-P".format(
                                self.expirations_pair[self.selected_expirations[1]],
                                self.otm_put[
                                    f"BTC-{self.expirations_pair[self.selected_expirations[1]]}"][0]), amount=0.1)
                            await self.place_order_sell(instrument_name="BTC-{0}-{1}-C".format(
                                self.expirations_pair[self.selected_expirations[0]],
                                self.otm_call[
                                    f"BTC-{self.expirations_pair[self.selected_expirations[0]]}"][0]), amount=0.1)
                            await self.place_order_buy(instrument_name="BTC-{0}-{1}-P".format(
                                self.expirations_pair[self.selected_expirations[0]],
                                self.otm_put[
                                    f"BTC-{self.expirations_pair[self.selected_expirations[0]]}"][0]), amount=0.1)
                            print("LONG EXECUTE!!!")
                            self.enabled = False
                            continue
                else:
                    continue

            except Exception as e:
                print(f"⚠️ Error processing while simulation: {e}")
                continue

class Telegram_bot(Strategy_RR):
    def __init__(self, *args, bot_token, **kwargs):
        super().__init__(*args, **kwargs)
        self.bot_token = bot_token
        self.loop.create_task(self.initialize_telegram_bot())
        self.start()

    # **1. Start Command**
    async def telegram_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text(
            "Welcome to the Trading Bot! 🚀\n"
            "Use the following commands:\n"
            "/trade <buy/sell> <amount> <instrument> [price] - Place an order\n"
            "/cancel <order_id> - Cancel an order\n"
            "/margin - Check margin & P&L\n"
            "/positions - Check current positions\n"
            "/toggle_risk <on/off> - Enable/Disable Risk Reversal Trading\n"
            "/set_expiration <DDMMMYY,DDMMMYY,...> - Update Expiration Dates\n"
        )

    # **2. Place an Order**
    async def trade(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            args = context.args
            if len(args) < 3:
                await update.message.reply_text("Usage: /trade <buy/sell> <amount> <instrument> [price]")
                return

            direction = args[0].lower()
            amount = float(args[1])
            instrument = args[2]
            price = float(args[3]) if len(args) > 3 else None

            if direction not in ["buy", "sell"]:
                await update.message.reply_text("Invalid direction. Use 'buy' or 'sell'.")
                return

            if direction == 'buy':
                await self.place_order_buy(instrument, amount, price)
            elif direction == 'sell':
                await self.place_order_sell(instrument, amount, price)

            await update.message.reply_text("✅ Order placed!")

        except Exception as e:
            await update.message.reply_text(f"⚠️ Error: {str(e)}")

    # **3. Cancel an Order**
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        try:
            args = context.args
            if len(args) < 1:
                await update.message.reply_text("Usage: /cancel <order_id>")
                return

            order_id = args[0]
            await self.cancel_order(order_id)
            await update.message.reply_text("❌ Order Canceled")

        except Exception as e:
            await update.message.reply_text(f"⚠️ Error: {str(e)}")

    # **4. Check Margin & P&L**
    async def margin(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        margin_data = self.portfolio_status['result']
        print(f"margin_data? : {margin_data}")
        await update.message.reply_text(
            f"📊 Portfolio Summary:\n"
            f"🔹 projected Equity: {margin_data['equity']} BTC\n"
            f"🔹 projected maintenance margin: {margin_data['projected_maintenance_margin']} BTC\n"
            f"🔹 projected initial margin: {margin_data['projected_initial_margin']} BTC\n"
            f"🔹 Available Margin: {margin_data['margin_balance']} BTC\n"
            f"🔹 P&L: {margin_data['total_pl']} BTC"
        )

    # **5. Toggle Risk Reversal Trading**
    async def toggle_risk_reversal(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        args = context.args
        if len(args) < 1:
            await update.message.reply_text("Usage: /toggle_risk <on/off>")
            return
        enabled = args[0].lower() == "on"
        self.enabled = enabled
        print(f"enabled? : {self.enabled}")
        await update.message.reply_text(f"⚙️ Risk Reversal is now {'ENABLED' if enabled else 'DISABLED'}.")


    async def initialize_telegram_bot(self):
        # build the application
        app = ApplicationBuilder().token(self.bot_token).build()

        app.add_handler(CommandHandler("start", self.telegram_start))
        app.add_handler(CommandHandler("trade", self.trade))
        app.add_handler(CommandHandler("cancel", self.cancel))
        app.add_handler(CommandHandler("margin", self.margin))
        app.add_handler(CommandHandler("toggle_risk", self.toggle_risk_reversal))

        await app.initialize()
        await app.start()
        await app.updater.start_polling()

        print("✅ Telegram bot is running...")


if __name__ == "__main__":
    # Logging
    logging.basicConfig(
        level='INFO',
        format='%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
        )

    # Connect to MySQL**
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="btc_options_db"
    )
    cursor = conn.cursor()

    with open('key/client_key.txt', 'r') as f:
        client_id = f.readline().strip()

    # Load the private key from the PEM file
    with open('key/private.pem', 'rb') as private_pem:
        private_key = serialization.load_pem_private_key(private_pem.read(), password=None)
    # Generate a timestamp
    timestamp = round(datetime.now().timestamp() * 1000)
    # Generate a **secure random nonce**
    nonce = secrets.token_hex(16)  # 16-byte hex string
    # Empty data field
    data = ""
    # Prepare the data to sign
    data_to_sign = bytes('{}\n{}\n{}'.format(timestamp, nonce, data), "latin-1")
    # Sign the data using the RSA private key with padding and hashing algorithm
    signature = private_key.sign(
        data_to_sign,
        padding.PKCS1v15(),
        hashes.SHA256()
    )
    # Encode the signature to base64
    ws_url = "wss://www.deribit.com/ws/api/v2"
    encoded_signature = base64.urlsafe_b64encode(signature).decode('utf-8').rstrip('=')

    # Telegram bot token
    with open('key/bot_token.txt', 'r') as f:
        bot_token = f.readline().strip()

    test = Telegram_bot(ws_url, client_id, timestamp, encoded_signature, nonce, data, bot_token=bot_token)
