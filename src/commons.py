from __future__ import annotations
from cmath import nan
from typing import NamedTuple
from datetime import datetime
from collections import OrderedDict
import numpy as np


class LabelsSubsets:
    INTERPOLATED = [
        'open',
        'high',
        'low',
        'close',
        'volume',
        'quote_asset_volume',
        'number_of_trades',
        'taker_buy_base_asset_volume',
        'taker_buy_quote_asset_volume',
    ]

class Format:
    KLINESRAW = OrderedDict([
        ('open_time', 'int64'),
        ('open', 'float64'),
        ('high', 'float64'),
        ('low', 'float64'),
        ('close', 'float64'),
        ('volume', 'float64'),
        ('quote_asset_volume', 'float64'),
        ('number_of_trades', 'int64'),
        ('taker_buy_base_asset_volume', 'float64'),
        ('taker_buy_quote_asset_volume', 'float64'),
    ])

    KLINESPROCESSING = OrderedDict([
        ('open_time', 'datetime64[ns]'),
        ('open', 'float64'),
        ('high', 'float64'),
        ('low', 'float64'),
        ('close', 'float64'),
        ('volume', 'float64'),
        ('quote_asset_volume', 'float64'),
        ('number_of_trades', 'int64'),
        ('taker_buy_base_asset_volume', 'float64'),
        ('taker_buy_quote_asset_volume', 'float64'),
    ])

    POSTPROCESSED = OrderedDict([
        ('open_time', 'datetime64[ns]'),
        ('open', 'float64'),
        ('high', 'float64'),
        ('low', 'float64'),
        ('close', 'float64'),
        ('volume', 'float64'),
        ('quote_asset_volume', 'float64'),
        ('number_of_trades', 'int64'),
        ('taker_buy_base_asset_volume', 'float64'),
        ('taker_buy_quote_asset_volume', 'float64'),
        ('interpolated', 'bool')
    ])


class KlinesRawRow(NamedTuple):
    open_time: int  #  timestamp in miliseconds since epoch
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_asset_volume: float
    number_of_trades: int
    taker_buy_base_asset_volume: float
    taker_buy_quote_asset_volume: float


class PostProcessedRow(NamedTuple):
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_asset_volume: float
    number_of_trades: int
    taker_buy_base_asset_volume: float
    taker_buy_quote_asset_volume: float
    interpolated: bool

    @staticmethod
    def from_pd_namedtuple(tpl: "PandasNamedTuple", interpolated: bool) -> PostProcessedRow:
        return PostProcessedRow(
            open_time=tpl.open_time,
            open=tpl.open,
            high=tpl.high,
            low=tpl.low,
            close=tpl.close,
            volume=tpl.volume,
            quote_asset_volume=tpl.quote_asset_volume,
            number_of_trades=tpl.number_of_trades,
            taker_buy_base_asset_volume=tpl.taker_buy_base_asset_volume,
            taker_buy_quote_asset_volume=tpl.taker_buy_quote_asset_volume,
            interpolated=interpolated
        )
    
    @staticmethod
    def interpolation_blank(open_time: datetime) -> PostProcessedRow:
        return PostProcessedRow(
            open_time=open_time,
            open=np.nan,
            high=np.nan,
            low=np.nan,
            close=np.nan,
            volume=np.nan,
            quote_asset_volume=np.nan,
            number_of_trades=np.nan,
            taker_buy_base_asset_volume=np.nan,
            taker_buy_quote_asset_volume=np.nan,
            interpolated=True
        )
