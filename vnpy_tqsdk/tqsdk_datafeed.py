from datetime import timedelta
from typing import Dict, List, Optional, Callable
import traceback
from pandas import Timestamp
from tqsdk import TqApi, TqAuth

from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.utility import ZoneInfo

INTERVAL_VT2TQ: Dict[Interval, int] = {Interval.MINUTE: 60, Interval.HOUR: 60 * 60,
                                       Interval.DAILY: 60 * 60 * 24, }

CHINA_TZ = ZoneInfo("Asia/Shanghai")

MAIN_SUFFIX = ["888", "88"]
INDEX_SUFFIX = ["999", "99"]
MAIN_SUFFIX_OLD = "L"
INDEX_SUFFIX_OLD = "I"


class TqsdkDatafeed(BaseDatafeed):
    """天勤TQsdk数据服务接口"""

    def __init__(self):
        """"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]
        self.api = TqApi(auth=TqAuth(self.username, self.password))

    def query_bar_history(
            self, req: HistoryRequest, output: Callable = print
    ) -> Optional[List[BarData]]:
        """查询k线数据"""
        # 初始化API
        try:
            api = self.api
            if not api:
                api: TqApi = TqApi(auth=TqAuth(self.username, self.password))
        except Exception:
            output(traceback.format_exc())
            return None

        # 标准化合约代码的大小写
        if req.exchange is Exchange.CZCE or req.exchange is Exchange.CFFEX:
            req.symbol = req.symbol.upper()
        else:
            req.symbol = req.symbol.lower()
        symbol = req.symbol  # 形如 DCE.a888

        # 查询数据
        # 指数DCE.a999 要转换成 KQ.i@DCE.a
        # 主连DCE.a888 要转换成 KQ.m@DCE.a
        for count, word in enumerate(symbol):
            if word.isdigit():
                break
        product: str = symbol[:count]
        time_str: str = symbol[count:]

        if time_str in MAIN_SUFFIX:
            if req.exchange is Exchange.CZCE or req.exchange is Exchange.CFFEX:
                tq_symbol: str = "KQ.m@{}.{}".format(
                    req.exchange.value, product.upper()
                )
            else:
                tq_symbol: str = "KQ.m@{}.{}".format(
                    req.exchange.value, product.lower()
                )
        elif time_str in INDEX_SUFFIX:
            if req.exchange is Exchange.CZCE or req.exchange is Exchange.CFFEX:
                tq_symbol: str = "KQ.i@{}.{}".format(
                    req.exchange.value, product.upper()
                )
            else:
                tq_symbol: str = "KQ.i@{}.{}".format(
                    req.exchange.value, product.lower()
                )
        else:
            tq_symbol: str = f"{req.exchange.value}.{symbol}"

        interval: str = INTERVAL_VT2TQ.get(req.interval, None)
        if not interval:
            output(f"Tqsdk查询K线数据失败：不支持的时间周期{req.interval.value}")
            return []

        # 时区的问题
        if req.start.tzinfo is None and req.end.tzinfo is None:
            req.start = req.start.replace(tzinfo=CHINA_TZ)
            req.end = req.end.replace(tzinfo=CHINA_TZ)
        elif req.start.tzinfo is not None and req.end.tzinfo is None:
            req.end = req.end.replace(tzinfo=req.start.tzinfo)
        elif req.start.tzinfo is None and req.end.tzinfo is not None:
            req.start = req.start.replace(tzinfo=req.end.tzinfo)
        # 最多获取10000条数据
        data_length: int = (
                int((req.end - req.start).total_seconds() / INTERVAL_VT2TQ[req.interval]) or 1)
        if data_length > 10000:
            data_length = 10000

        print("tqsdk: get_kline_serial, params:")
        print(f"symbol: {tq_symbol}")
        print(f"req: {req}")
        print(f"duration_seconds: {interval}")
        print(f"data_length: {data_length}")
        df: pd.DataFrame = api.get_kline_serial(
            symbol=tq_symbol, duration_seconds=interval, data_length=data_length
        )
        # 去除nan
        df.dropna(inplace=True)

        # 关闭API
        # api.close()

        # 解析数据
        bars: List[BarData] = []

        if df is not None:
            for tp in df.itertuples():
                # 天勤时间为与1970年北京时间相差的秒数，需要加上8小时差转为utc时间
                dt: Timestamp = Timestamp(tp.datetime).to_pydatetime() + timedelta(
                    hours=8
                )

                bar: BarData = BarData(
                    symbol=req.symbol, exchange=req.exchange, interval=req.interval,
                    datetime=dt.replace(tzinfo=CHINA_TZ), open_price=tp.open, high_price=tp.high,
                    low_price=tp.low, close_price=tp.close, volume=tp.volume,
                    open_interest=tp.open_oi, gateway_name="TQ", )
                bars.append(bar)

        return bars

    def close(self):
        """关闭API
        因为tqsdk是长连接，添加此函数，和vnpy-DataManger窗口的关闭信号连接
        """
        if self.api:
            self.api.close()
            self.api = None
