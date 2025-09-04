import os
import time
import json
import requests
from typing import Optional, Dict, Any, Tuple
import pandas as pd

DEFAULT_TIMEOUT = 30


class TwcClient:

    def __init__(
        self,
        hod_api_key: Optional[str] = None,
        org_id: Optional[str] = None,
        saas_client_id: Optional[str] = None,
        geospatial_client_id: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self.hod_api_key = hod_api_key or os.getenv("HOD_API_KEY", "").strip()
        self.org_id = org_id or os.getenv("ORG_ID", "").strip()
        self.saas_client_id = saas_client_id or os.getenv("SAAS_CLIENT_ID", "").strip()
        # GEOSPATIAL_CLIENT_ID 未設定なら TENANT_ID から geospatial-<TENANT_ID> を推測
        self.geospatial_client_id = (
            geospatial_client_id or os.getenv("GEOSPATIAL_CLIENT_ID", "").strip()
            or (("geospatial-" + os.getenv("TENANT_ID", "").strip()) if os.getenv("TENANT_ID") else "")
        )
        self.timeout = timeout
        self._jwt: Optional[str] = None
        self._jwt_time: float = 0.0

    # --- 認証 ---
    def ensure_jwt(self, force: bool = False) -> str:
        now = time.time()
        if (not force) and self._jwt and (now - self._jwt_time < 50 * 60):
            return self._jwt
        url = "https://api.ibm.com/saascore/run/authentication-retrieve/api-key"
        params = {"orgId": self.org_id}
        headers = {"x-api-key": self.hod_api_key, "x-ibm-client-Id": self.saas_client_id}
        resp = requests.get(url, params=params, headers=headers, timeout=self.timeout)
        resp.raise_for_status()
        txt = resp.text.strip()
        if txt.startswith('"') and txt.endswith('"'):
            txt = txt[1:-1]
        self._jwt = txt
        self._jwt_time = now
        return self._jwt

    # --- API 呼び出し ---
    def hod_direct(
        self,
        latitude: float,
        longitude: float,
        start_iso: str,
        end_iso: str,
        units: str = "m",
        products: str = "all",
    ) -> Dict[str, Any]:
        jwt = self.ensure_jwt()
        base = "https://api.ibm.com/geospatial/run/v3/wx/hod/r1/direct"
        params = {
            "format": "json",
            "products": products,
            "geocode": f"{latitude:.5f},{longitude:.5f}",
            "startDateTime": start_iso,
            "endDateTime": end_iso,
            "units": units,
        }
        headers = {"authorization": f"Bearer {jwt}", "x-ibm-client-Id": self.geospatial_client_id}
        resp = requests.get(base, params=params, headers=headers, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # JSON ファイル読み込み（オフライン用） ---
    @staticmethod
    def read_json_file(path_or_file) -> Dict[str, Any]:
        if hasattr(path_or_file, "read"):
            return json.load(path_or_file)
        with open(path_or_file, "r", encoding="utf-8") as f:
            return json.load(f)

    # --- 返答 → DataFrame 変換 ---
    @staticmethod
    def _from_series_dict(payload: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        weather.json のように、各キーが等長の配列になっている形を
        時系列の行テーブルに展開する。
        """
        list_keys = [k for k, v in payload.items() if isinstance(v, list)]
        if not list_keys:
            return None
        n = max(len(payload[k]) for k in list_keys)
        # 主要キーがすべて同じ長さならテーブル化
        if all(isinstance(payload.get(k), list) and len(payload[k]) == n for k in list_keys):
            rows = []
            for i in range(n):
                row = {}
                for k in list_keys:
                    row[k] = payload[k][i]
                rows.append(row)
            return pd.DataFrame(rows)
        return None

    @staticmethod
    def to_dataframe(resp: Dict[str, Any]) -> pd.DataFrame:
        """
        - list や {'data':[...]} 等は json_normalize
        - dict-of-lists（weather.json 形式）は _from_series_dict で縦持ちへ
        その後、timestamp / date を整備。
        """
        df = None

        # dict-of-lists に最初に対応
        if isinstance(resp, dict):
            cand = TwcClient._from_series_dict(resp)
            if cand is not None:
                df = cand

        # それ以外の一般形
        if df is None:
            if isinstance(resp, list):
                data = resp
            elif isinstance(resp, dict):
                for key in ("data", "observations", "series", "result", "results"):
                    if key in resp and isinstance(resp[key], list):
                        data = resp[key]
                        break
                else:
                    data = [resp]
            else:
                data = [resp]
            df = pd.json_normalize(data, max_level=1)

        # 時刻列の決定（UTC を前提に取り込んで JST に変換）
        for cand in ["validTimeLocal", "validTimeUtc", "fcstValidLocal", "fcstValidUtc", "time", "validTime"]:
            if cand in df.columns:
                df["timestamp"] = pd.to_datetime(df[cand], errors="coerce", utc=True)
                break
        if "timestamp" in df.columns:
            df["date"] = df["timestamp"].dt.tz_convert("Asia/Tokyo").dt.date
            df["datetime_jst"] = df["timestamp"].dt.tz_convert("Asia/Tokyo")

        return df


# --- 日付ユーティリティ（UTC境界） ---
def month_bounds(year: int, month: int) -> Tuple[str, str]:
    from datetime import datetime, timezone
    import calendar
    first = datetime(year, month, 1, 0, 0, 0, tzinfo=timezone.utc)
    last_day = calendar.monthrange(year, month)[1]
    last = datetime(year, month, last_day, 23, 59, 59, tzinfo=timezone.utc)
    return first.isoformat().replace("+00:00", "Z"), last.isoformat().replace("+00:00", "Z")


# --- 日付ユーティリティ（ローカル境界：JSTなど） ---
def month_bounds_local(year: int, month: int, tz_str: str = "Asia/Tokyo") -> Tuple[str, str]:
    """
    指定タイムゾーンでの「その月」の 00:00:00 — 23:59:59 を、
    UTC に変換
    """
    from datetime import datetime, timezone
    from zoneinfo import ZoneInfo
    import calendar

    tz = ZoneInfo(tz_str)
    first_local = datetime(year, month, 1, 0, 0, 0, tzinfo=tz)
    last_day = calendar.monthrange(year, month)[1]
    last_local = datetime(year, month, last_day, 23, 59, 59, tzinfo=tz)

    first_utc = first_local.astimezone(timezone.utc)
    last_utc = last_local.astimezone(timezone.utc)
    return (
        first_utc.isoformat().replace("+00:00", "Z"),
        last_utc.isoformat().replace("+00:00", "Z"),
    )


def clamp_to_month_local(df: pd.DataFrame, year: int, month: int, tz_str: str = "Asia/Tokyo") -> pd.DataFrame:
    """
    DataFrame を「指定タイムゾーンにおける指定年/月」にだけ絞り込む（上限は翌月の月初）。
    'date' も指定TZで再生成する。
    """
    if df.empty:
        return df.copy()

    from zoneinfo import ZoneInfo
    tz = ZoneInfo(tz_str)

    # 基準となる時刻列を決定
    if "timestamp" in df.columns:
        ts = df["timestamp"]
        ts = pd.to_datetime(ts, errors="coerce", utc=True)
    elif "datetime_jst" in df.columns:
        ts = pd.to_datetime(df["datetime_jst"], errors="coerce").dt.tz_convert("Asia/Tokyo").dt.tz_convert("UTC")
    else:
        for cand in ["validTimeLocal", "validTimeUtc", "fcstValidLocal", "fcstValidUtc", "time", "validTime"]:
            if cand in df.columns:
                ts = pd.to_datetime(df[cand], errors="coerce", utc=True)
                break
        else:
            return df.copy()

    # ローカルTZへ
    ts_local = ts.dt.tz_convert(tz)

    # 月の範囲 [start, next_start)
    start = pd.Timestamp(year=year, month=month, day=1, tz=tz)
    if month == 12:
        next_start = pd.Timestamp(year=year + 1, month=1, day=1, tz=tz)
    else:
        next_start = pd.Timestamp(year=year, month=month + 1, day=1, tz=tz)

    mask = (ts_local >= start) & (ts_local < next_start)
    out = df.loc[mask].copy()

    # 指定TZで 'date' を再生成（JSTなら date がJSTに一致）
    out["date"] = ts_local.loc[mask].dt.date
    out["datetime_local"] = ts_local.loc[mask]
    if tz_str == "Asia/Tokyo":
        out["datetime_jst"] = out["datetime_local"]

    return out


# --- 日別集計（既存） ---
def daily_aggregates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    t = "temperature" if "temperature" in df.columns else None
    h = "relativeHumidity" if "relativeHumidity" in df.columns else None

    # 降水：24時間値があれば「日内最大値」、なければ1時間値を「日合計」
    p_col, p_op = None, None
    if "precip24Hour" in df.columns:
        p_col, p_op = "precip24Hour", "max"
    elif "precip1Hour" in df.columns:
        p_col, p_op = "precip1Hour", "sum"

    if "date" not in df.columns and "timestamp" in df.columns:
        df["date"] = df["timestamp"].dt.tz_convert("Asia/Tokyo").dt.date

    mapping = {}
    if t: mapping["temp"] = (t, "mean")       # 気温は日平均
    if h: mapping["humidity"] = (h, "mean")   # 湿度は日平均
    if p_col: mapping["precip"] = (p_col, p_op)

    out = df.groupby("date").agg(**mapping).reset_index()

    for col in ["temp", "humidity"]:
        if col in out.columns:
            out[col] = out[col].round(1)

    return out


# --- 10日ごと集計（既存） ---
def ten_day_buckets(daily_df: pd.DataFrame) -> pd.DataFrame:
    """
    1–10 / 11–20 / 21–末 の3区間で集計。
      - 気温・湿度: 平均
      - 降水量: 合計
    入力は daily_aggregates() の返却 DataFrame を想定。
    """
    import pandas as pd
    if daily_df is None or daily_df.empty or "date" not in daily_df.columns:
        return pd.DataFrame()

    g = daily_df.copy()
    g["day"] = pd.to_datetime(g["date"]).dt.day

    def bucket(d: int) -> str:
        if d <= 10: return "1–10"
        if d <= 20: return "11–20"
        return "21–末"

    g["bucket"] = g["day"].apply(bucket)

    agg_map = {}
    if "temp" in g.columns: agg_map["temp"] = "mean"
    if "humidity" in g.columns: agg_map["humidity"] = "mean"
    if "precip" in g.columns: agg_map["precip"] = "sum"

    if not agg_map:
        return g[["bucket"]].drop_duplicates().sort_values("bucket").reset_index(drop=True)

    out = g.groupby("bucket", as_index=False).agg(agg_map)

    try:
        from pandas import CategoricalDtype
        order = CategoricalDtype(categories=["1–10", "11–20", "21–末"], ordered=True)
        out["bucket"] = out["bucket"].astype(order)
        out = out.sort_values("bucket").reset_index(drop=True)
    except Exception:
        pass

    for col in ["temp", "humidity"]:
        if col in out.columns:
            out[col] = out[col].round(1)
    return out


# --- 販売数らしい列を推定 ---
def guess_sales_column(df: pd.DataFrame) -> Optional[str]:
    """
    結合テーブルから「売れた個数」に該当しそうな列名を推定して返す。
    1) 優先候補の完全一致を数値列から探索 → 2) 日本語の含意（売/販/出荷 × 数/個/量）
       → 3) それでも無ければ、天気列を除いた最初の数値列
    """
    if df is None or df.empty:
        return None

    # 天気・標準列は除外
    exclude = {"date", "temp", "humidity", "precip", "temperature", "relativeHumidity",
               "precip1Hour", "precip24Hour"}
    numeric_cols = [c for c in df.columns
                    if c not in exclude and pd.api.types.is_numeric_dtype(df[c])]

    if not numeric_cols:
        return None

    priority = [
        "売れた個数", "売上個数", "販売個数", "販売数量", "売上数", "販売数",
        "個数", "数量",
        "units_sold", "qty_sold", "sold", "sales_qty", "sales", "出荷数",
    ]

    lower_map = {c: str(c).lower() for c in numeric_cols}
    for name in priority:
        name_l = name.lower()
        for c, lc in lower_map.items():
            if lc == name_l:
                return c

    # 日本語の含意（「売/販/出荷」かつ「数/個/量」を含む）
    for c in numeric_cols:
        s = str(c)
        if any(k in s for k in ["売", "販", "出荷"]) and any(k in s for k in ["数", "個", "量"]):
            return c

    return numeric_cols[0]
