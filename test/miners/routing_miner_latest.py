import time
from typing import Tuple

import bittensor as bt
import pandas as pd
import httpx
import json

from protocol import Challenge
from utils.cm_data import CMData
from utils.timestamp import get_before, to_datetime, to_str
import random


API_ALGO_DICT_FILE_NAME = "api_and_algo.json"


def get_point_estimate(cm: CMData, timestamp: str) -> float:
    """Make a naive forecast by predicting the most recent price

    Args:
        cm (CMData): The CoinMetrics API client
        timestamp (str): The current timestamp provided by the validator request

    Returns:
        (float): The current BTC price tied to the provided timestamp
    """

    # Ensure timestamp is correctly typed and set to UTC
    provided_timestamp = to_datetime(timestamp)

    # Query CM API for a pandas dataframe with only one record
    price_data: pd.DataFrame = cm.get_CM_ReferenceRate(
        assets="BTC",
        start=None,
        end=to_str(provided_timestamp),
        frequency="1s",
        limit_per_asset=1,
        paging_from="end",
        use_cache=True,
    )

    # Get current price closest to the provided timestamp
    btc_price: float = float(price_data["ReferenceRateUSD"].iloc[-1])

    # Return the current price of BTC as our point estimate
    return btc_price


def get_prediction_interval(cm: CMData, timestamp: str, point_estimate: float) -> Tuple[float, float]:
    """Make a naive multi-step prediction interval by estimating
    the sample standard deviation

    Args:
        cm (CMData): The CoinMetrics API client
        timestamp (str): The current timestamp provided by the validator request
        point_estimate (float): The center of the prediction interval

    Returns:
        (float): The 90% naive prediction interval lower bound
        (float): The 90% naive prediction interval upper bound

    Notes:
        Make reasonable assumptions that the 1s BTC price residuals are
        uncorrelated and normally distributed
    """

    # Set the time range to be 24 hours
    # Ensure both timestamps are correctly typed and set to UTC
    start_time = get_before(timestamp, days=1, minutes=0, seconds=0)
    end_time = to_datetime(timestamp)

    # Query CM API for sample standard deviation of the 1s residuals
    historical_price_data: pd.DataFrame = cm.get_CM_ReferenceRate(
        assets="BTC", start=to_str(start_time), end=to_str(end_time), frequency="1s"
    )
    last_price: float = float(historical_price_data["ReferenceRateUSD"].iloc[-1])

    # Calculate the lower bound and upper bound
    lower_bound: float = last_price - 150 + random.uniform(-25, 25)
    upper_bound: float = last_price + 150 + random.uniform(-25, 25)

    # Return the naive prediction interval for our forecast
    return lower_bound, upper_bound


def compose_payload(synapse: Challenge, coldkey_name: str, hotkey_name: str):
    key = f"{coldkey_name}_{hotkey_name}"
    try:
        with open(API_ALGO_DICT_FILE_NAME, "r") as f:
            data_all = json.load(f)
    except FileNotFoundError:
        bt.logging.error(f"{API_ALGO_DICT_FILE_NAME} not found.")
        return {}
    except json.JSONDecodeError as e:
        bt.logging.error(f"Invalid JSON in {API_ALGO_DICT_FILE_NAME}: {e}")
        return {}
    except Exception as e:
        bt.logging.error(f"Could not open {API_ALGO_DICT_FILE_NAME}: {e}")
        return {}

    data = data_all.get(key)
    if not data:
        bt.logging.error(f"No entry found in {API_ALGO_DICT_FILE_NAME} for {key}.")
        return {}

    api_server_url = data.get("api_server_url") or ""
    extra = data.get("extra") or {}
    if not api_server_url or not extra:
        bt.logging.error(f"Entry for {key} is missing required fields.")
        return {}

    return {
        "api_server_url": api_server_url,
        "timestamp": synapse.timestamp,
        "assets": ["btc"],
        "extra": extra,
    }


async def forward(synapse: Challenge, cm: CMData, coldkey_name: str, hotkey_name: str, miner_id: str) -> Challenge:
    total_start_time = time.perf_counter()
    bt.logging.info(
        f"👈 Received prediction request from: {synapse.dendrite.hotkey} for timestamp: {synapse.timestamp}"
    )
    payload = compose_payload(synapse, coldkey_name, hotkey_name)
    api_server_url = payload.get("api_server_url", "")

    ok = False
    point_estimate, prediction_interval = 111111.0, []
    try:
        if api_server_url == "":
            raise ValueError("api_server_url is empty.")
        api_endpoint = api_server_url.rstrip("/") + "/point_and_interval"
        async with httpx.AsyncClient() as client:
            r = await client.post(api_endpoint, json=payload, timeout=8)
            r.raise_for_status()
            response = r.json()
        if r.status_code == 200 and response.get("ok", False) == True:
            result = response["result"]["btc"]
            point_estimate = float(result["pred_price"])
            prediction_interval = result["pred_interval"]
            ok = True
            bt.logging.info(f"✅ Successfully fetched prediction from custom API.")
        else:
            bt.logging.warning(f"Custom API returned non-200 status or ok=False: {response}")
    except Exception as e:
        bt.logging.warning(f"Failed to fetch prediction from custom API: {e}")
        pass

    if ok == False or point_estimate == 111111.0 or len(prediction_interval) != 2:
        bt.logging.warning("Since my custom api returned False, I am going to use the naive forecast.")
        point_estimate_start = time.perf_counter()
        # Get the naive point estimate
        point_estimate: float = get_point_estimate(cm=cm, timestamp=synapse.timestamp)

        point_estimate_time = time.perf_counter() - point_estimate_start
        bt.logging.debug(f"⏱️ Point estimate function took: {point_estimate_time:.3f} seconds")

        interval_start = time.perf_counter()
        # Get the naive prediction interval
        prediction_interval: Tuple[float, float] = get_prediction_interval(
            cm=cm, timestamp=synapse.timestamp, point_estimate=point_estimate
        )

        interval_time = time.perf_counter() - interval_start
        bt.logging.debug(f"⏱️ Prediction interval function took: {interval_time:.3f} seconds")

    synapse.prediction = point_estimate
    synapse.interval = prediction_interval

    total_time = time.perf_counter() - total_start_time
    bt.logging.debug(f"⏱️ Total forward call took: {total_time:.3f} seconds")

    if synapse.prediction is not None:
        bt.logging.success(f"Predicted price: {synapse.prediction}  |  Predicted Interval: {synapse.interval}")
    else:
        bt.logging.info("No prediction for this request.")
    return synapse
