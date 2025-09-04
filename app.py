import os
from pathlib import Path

import streamlit as st
import pandas as pd
from dotenv import load_dotenv

from twc import (
    TwcClient,
    month_bounds_local,
    daily_aggregates,
    clamp_to_month_local,
    ten_day_buckets,
    guess_sales_column,
)

APP_DIR = Path(__file__).parent

# 都市 → 座標（WGS84）
CITY_COORDS = {
    "東京": (35.6812, 139.7671),   # 東京駅
    "大阪": (34.6937, 135.5023),   # 大阪市役所付近
    "福岡": (33.5902, 130.4017),   # 天神付近
}


def read_weather_json_from_known_places() -> dict:
    candidates = [
        APP_DIR / "weather.json",
        APP_DIR / "data" / "weather.json",
        Path("/mnt/data/weather.json"),
    ]
    for p in candidates:
        if p.exists():
            return TwcClient.read_json_file(p)
    return {}


def prepare_inventory_daily(inv_df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """在庫CSVを日次に整形：日付検出→指定年月にクランプ→数値列は日別合計"""
    if inv_df is None or inv_df.empty:
        return pd.DataFrame()

    # 日付列（日本語想定）
    if "日付" not in inv_df.columns:
        raise ValueError("在庫CSVに『日付』列がありません。")

    x = inv_df.copy()
    x["date"] = pd.to_datetime(x["日付"], errors="coerce").dt.date
    x = x[x["date"].notna()]

    # 数値に見える object 列を明示変換（例: '1,234' や空白混在）
    for c in x.columns:
        if c not in ("date", "日付") and pd.api.types.is_object_dtype(x[c]):
            x[c] = pd.to_numeric(x[c].astype(str).str.replace(",", ""), errors="coerce")

    # 年/月で抽出
    if year and month:
        dt = pd.to_datetime(x["date"])
        x = x[(dt.dt.year == int(year)) & (dt.dt.month == int(month))]

    # 日次化（数値列を日合計）
    num_cols = [c for c in x.columns if c not in ("date", "日付") and pd.api.types.is_numeric_dtype(x[c])]
    if num_cols:
        inv_daily = x.groupby("date", as_index=False)[num_cols].sum()
    else:
        inv_daily = x[["date"]].drop_duplicates()

    return inv_daily


# ==============================
# 既存：日別の重ねグラフ
# ==============================
def draw_overlaid_chart(daily: pd.DataFrame) -> None:
    """
    日次の temp(℃), humidity(%), precip(mm) を1枚に重ね描画。
      - 左軸: 気温(折れ線)
      - 右軸(オフセット0px): 降水量(棒)
      - 右軸(オフセット40px): 湿度(破線)
    列が無いものは自動スキップ。
    """
    if daily is None or daily.empty or "date" not in daily.columns:
        st.caption("グラフ化できる日次データがありません。")
        return

    df = daily.copy()
    df["date"] = pd.to_datetime(df["date"])

    has_temp = "temp" in df.columns
    has_hum  = "humidity" in df.columns
    has_prcp = "precip" in df.columns

    layers = []
    if has_temp:
        layers.append({
            "mark": {"type": "line", "point": True},
            "encoding": {"y": {"field": "temp", "type": "quantitative", "axis": {"title": "気温 (℃)"}}},
        })
    if has_prcp:
        layers.append({
            "mark": {"type": "bar", "opacity": 0.35},
            "encoding": {"y": {"field": "precip", "type": "quantitative",
                               "axis": {"title": "降水量 (mm)", "orient": "right", "offset": 0}}},
        })
    if has_hum:
        layers.append({
            "mark": {"type": "line", "strokeDash": [4, 2]},
            "encoding": {"y": {"field": "humidity", "type": "quantitative",
                               "axis": {"title": "湿度 (%)", "orient": "right", "offset": 40}}},
        })

    if not layers:
        st.caption("グラフ化できる列が見つかりませんでした。")
        return

    tooltip_fields = [{"field": "date", "type": "temporal", "title": "日付"}]
    if has_temp: tooltip_fields.append({"field": "temp", "type": "quantitative", "title": "気温(℃)", "format": ".1f"})
    if has_hum:  tooltip_fields.append({"field": "humidity", "type": "quantitative", "title": "湿度(%)", "format": ".0f"})
    if has_prcp: tooltip_fields.append({"field": "precip", "type": "quantitative", "title": "降水量(mm)", "format": ".1f"})

    spec = {
        "height": 320,
        "encoding": {"x": {"field": "date", "type": "temporal", "title": "日付"}, "tooltip": tooltip_fields},
        "layer": layers,
        "resolve": {"scale": {"y": "independent"}},
        "config": {"axis": {"labelFontSize": 11, "titleFontSize": 12}, "legend": {"labelFontSize": 11, "titleFontSize": 12}},
    }
    st.subheader("重ねて表示（気温・湿度・降水）")
    st.vega_lite_chart(df, spec, use_container_width=True)


# ==============================
# 追加：10日ごとグラフ
# ==============================
def draw_ten_day_chart(ten_df: pd.DataFrame) -> None:
    if ten_df is None or ten_df.empty or "bucket" not in ten_df.columns:
        st.caption("10日ごとに集計できるデータがありません。")
        return

    df = ten_df.copy()
    has_temp = "temp" in df.columns
    has_hum  = "humidity" in df.columns
    has_prcp = "precip" in df.columns

    layers = []
    if has_prcp:
        layers.append({
            "mark": {"type": "bar", "opacity": 0.35},
            "encoding": {"y": {"field": "precip", "type": "quantitative",
                               "axis": {"title": "降水量 (mm)", "orient": "right", "offset": 0}}}
        })
    if has_temp:
        layers.append({
            "mark": {"type": "line", "point": True},
            "encoding": {"y": {"field": "temp", "type": "quantitative", "axis": {"title": "気温 (℃)"}}}
        })
    if has_hum:
        layers.append({
            "mark": {"type": "line", "strokeDash": [4, 2]},
            "encoding": {"y": {"field": "humidity", "type": "quantitative",
                               "axis": {"title": "湿度 (%)", "orient": "right", "offset": 40}}}
        })

    tooltip = [{"field": "bucket", "type": "nominal", "title": "区間"}]
    if has_temp: tooltip.append({"field": "temp", "type": "quantitative", "title": "気温(℃)", "format": ".1f"})
    if has_hum:  tooltip.append({"field": "humidity", "type": "quantitative", "title": "湿度(%)", "format": ".0f"})
    if has_prcp: tooltip.append({"field": "precip", "type": "quantitative", "title": "降水量(mm)", "format": ".1f"})

    spec = {
        "height": 280,
        "encoding": {"x": {"field": "bucket", "type": "nominal", "title": "10日区間",
                           "sort": ["1–10", "11–20", "21–末"]},
                     "tooltip": tooltip},
        "layer": layers,
        "resolve": {"scale": {"y": "independent"}},
        "config": {"axis": {"labelFontSize": 11, "titleFontSize": 12}, "legend": {"labelFontSize": 11, "titleFontSize": 12}},
    }
    st.subheader("10日ごとに重ねて表示（気温・湿度・降水）")
    st.vega_lite_chart(df, spec, use_container_width=True)


# ==============================
# 修正：販売×天気（販売＝赤線、降水＝棒）
# ==============================
def draw_sales_weather_chart(joined: pd.DataFrame, sales_col: str) -> None:
    """
    左軸: 売れた個数（赤・実線の折れ線）
    右軸(0px): 降水（棒）
    右軸(40px): 湿度（破線）
    右軸(80px): 気温（折れ線）
    """
    if (joined is None or joined.empty or "date" not in joined.columns
            or not sales_col or sales_col not in joined.columns):
        st.caption("販売×天気の図を描画できるデータがありません。")
        return

    df = joined.copy()
    df["date"] = pd.to_datetime(df["date"])

    has_temp = "temp" in df.columns
    has_hum  = "humidity" in df.columns
    has_prcp = "precip" in df.columns

    layers = []
    # 降水＝棒（背面）
    if has_prcp:
        layers.append({
            "mark": {"type": "bar", "opacity": 0.25},
            "encoding": {"y": {"field": "precip", "type": "quantitative",
                               "axis": {"title": "降水量 (mm)", "orient": "right", "offset": 0}}}
        })
    # 湿度＝破線（右軸40px）
    if has_hum:
        layers.append({
            "mark": {"type": "line", "strokeDash": [4, 2]},
            "encoding": {"y": {"field": "humidity", "type": "quantitative",
                               "axis": {"title": "湿度 (%)", "orient": "right", "offset": 40}}}
        })
    # 気温＝線（右軸80px）
    if has_temp:
        layers.append({
            "mark": {"type": "line"},
            "encoding": {"y": {"field": "temp", "type": "quantitative",
                               "axis": {"title": "気温 (℃)", "orient": "right", "offset": 80}}}
        })
    # 売れた個数＝赤い実線（最前面・左軸）
    layers.append({
        "mark": {"type": "line", "strokeWidth": 2},
        "encoding": {
            "y": {"field": sales_col, "type": "quantitative", "axis": {"title": "売れた個数"}},
            "color": {"value": "red"},
        },
    })

    tooltip = [{"field": "date", "type": "temporal", "title": "日付"},
               {"field": sales_col, "type": "quantitative", "title": "売れた個数"}]
    if has_temp: tooltip.append({"field": "temp", "type": "quantitative", "title": "気温(℃)", "format": ".1f"})
    if has_hum:  tooltip.append({"field": "humidity", "type": "quantitative", "title": "湿度(%)", "format": ".0f"})
    if has_prcp: tooltip.append({"field": "precip", "type": "quantitative", "title": "降水量(mm)", "format": ".1f"})

    spec = {
        "height": 340,
        "encoding": {"x": {"field": "date", "type": "temporal", "title": "日付"}, "tooltip": tooltip},
        "layer": layers,
        "resolve": {"scale": {"y": "independent"}},
        "config": {"axis": {"labelFontSize": 11, "titleFontSize": 12}, "legend": {"labelFontSize": 11, "titleFontSize": 12}},
    }
    st.subheader("販売と天気の関係（売れた個数 × 気温・湿度・降水）")
    st.vega_lite_chart(df, spec, use_container_width=True)


def main():
    st.set_page_config(page_title="Weather Demo: 日別集計＋在庫結合", layout="wide")
    st.title("天気・気温・湿度（日別集計）＋ 在庫結合（任意）")

    load_dotenv()  # .env があれば読み込む

    # --- セッション状態の初期化（選択変更で再描画できるように） ---
    for k in ("daily_df", "ten_df", "joined_df"):
        if k not in st.session_state:
            st.session_state[k] = pd.DataFrame()

    with st.sidebar:
        st.header("モード / 入力")
        mode = st.radio("データ取得モード", ["オフライン（weather.json）", "オンライン（API使用）"], index=1)

        st.markdown("---")
        city = st.selectbox("都市", list(CITY_COORDS.keys()), index=0)
        st.caption("※ オフラインはファイル内容が優先されます。")

        uploaded_json = None
        if mode.startswith("オフライン"):
            st.markdown("---")
            uploaded_json = st.file_uploader("weather.json をアップロード（任意）", type=["json"])
            st.caption("未アップロードでも既定パスから自動探索します。")

        st.markdown("---")
        col_a, col_b = st.columns(2)
        with col_a:
            year = st.number_input("年", min_value=2020, max_value=2030, value=2025, step=1)
        with col_b:
            month = st.selectbox("月", list(range(1, 13)), index=5)  # 既定: 6月

        st.markdown("---")
        st.caption("API認証（IBM EIS/HOD）")
        hod_api_key = st.text_input("HOD_API_KEY", os.getenv("HOD_API_KEY", ""), type="password")
        org_id = st.text_input("ORG_ID", os.getenv("ORG_ID", ""))
        saas_client_id = st.text_input("SAAS_CLIENT_ID", os.getenv("SAAS_CLIENT_ID", ""))
        geospatial_client_id = st.text_input(
            "GEOSPATIAL_CLIENT_ID（未設定なら TENANT_ID を使用）",
            os.getenv("GEOSPATIAL_CLIENT_ID", ""),
        )

        st.markdown("---")
        use_stock = st.checkbox("在庫CSVを結合する（任意）", value=False)
        stock_file = None
        if use_stock:
            stock_file = st.file_uploader("在庫CSV（選択月分）をアップロード", type=["csv"])
            st.caption("想定: 'date' または '日付' 列がある日次CSV（YYYY-MM-DD 推奨）。")

        st.markdown("---")
        fetch = st.button("データ取得")

    # === データ取得（押したタイミングでのみ実行） ===
    if fetch:
        daily = pd.DataFrame()
        if mode.startswith("オフライン"):
            if uploaded_json is not None:
                payload = TwcClient.read_json_file(uploaded_json)
            else:
                payload = read_weather_json_from_known_places()
                if not payload:
                    st.error("weather.json が見つかりません。左側からアップロードしてください。")
                    st.stop()
            raw_df = TwcClient.to_dataframe(payload)
            raw_df = clamp_to_month_local(raw_df, int(year), int(month), "Asia/Tokyo")
            daily = daily_aggregates(raw_df)
        else:
            start_iso, end_iso = month_bounds_local(int(year), int(month), "Asia/Tokyo")
            lat, lon = CITY_COORDS[city]
            client = TwcClient(
                hod_api_key=hod_api_key,
                org_id=org_id,
                saas_client_id=saas_client_id,
                geospatial_client_id=geospatial_client_id,
            )
            try:
                resp = client.hod_direct(lat, lon, start_iso, end_iso, units="m", products="all")
                raw_df = TwcClient.to_dataframe(resp)
                raw_df = clamp_to_month_local(raw_df, int(year), int(month), "Asia/Tokyo")
                daily = daily_aggregates(raw_df)
            except Exception as e:
                st.error(f"API 呼び出しでエラー: {e}")

        # セッションに保存（後段の描画はボタン外で常時実行）
        st.session_state["daily_df"] = daily
        st.session_state["ten_df"] = ten_day_buckets(daily) if not daily.empty else pd.DataFrame()

        # 在庫結合（任意）
        joined = pd.DataFrame()
        if use_stock and stock_file is not None and not daily.empty:
            try:
                try:
                    inv_raw = pd.read_csv(stock_file)
                except UnicodeDecodeError:
                    stock_file.seek(0)
                    inv_raw = pd.read_csv(stock_file, encoding="cp932")
                inv_daily = prepare_inventory_daily(inv_raw, int(year), int(month))
                if not inv_daily.empty:
                    joined = pd.merge(daily, inv_daily, on="date", how="inner")
            except Exception as e:
                st.error(f"在庫CSVの処理でエラー: {e}")
        st.session_state["joined_df"] = joined

    # === ここからは常に描画（選択変更で即反映） ===
    st.subheader("日別の簡易集計")
    daily = st.session_state["daily_df"]
    if not daily.empty:
        st.dataframe(daily, use_container_width=True)
        draw_overlaid_chart(daily)
        st.download_button(
            "↑ 日別集計をCSVでダウンロード",
            daily.to_csv(index=False).encode("utf-8"),
            file_name="weather_daily.csv",
            mime="text/csv",
        )
        # 10日ごと
        st.markdown("---")
        st.subheader("10日ごとの集計（1–10 / 11–20 / 21–末）")
        ten = st.session_state["ten_df"]
        if ten.empty:
            st.caption("10日ごとの集計を作成できませんでした。")
        else:
            st.dataframe(ten, use_container_width=True)
            draw_ten_day_chart(ten)
            st.download_button(
                "↑ 10日ごとの集計をCSVでダウンロード",
                ten.to_csv(index=False).encode("utf-8"),
                file_name="weather_10day.csv",
                mime="text/csv",
            )
    else:
        st.info("左の設定を選んで「データ取得」を押してください。")

    # 在庫×天気（結合）＋ 販売×天気グラフ
    if use_stock:
        st.markdown("---")
        st.subheader("在庫×天気（結合データ）")
        joined = st.session_state["joined_df"]
        if joined is None or joined.empty:
            st.info("在庫CSVをアップロードしてから「データ取得」を押すと、ここに表示されます。")
        else:
            st.dataframe(joined, use_container_width=True)
            st.download_button(
                "↑ 在庫×天気（結合）をCSVでダウンロード",
                joined.to_csv(index=False).encode("utf-8"),
                file_name="weather_inventory.csv",
                mime="text/csv",
            )

            # 販売列の選択（fetch ボタン外なので選択反映される）
            st.markdown("#### 販売と天気の関係")
            weather_cols = {"temp", "humidity", "precip"}
            numeric_cols = [c for c in joined.columns
                            if c not in weather_cols and c != "date" and pd.api.types.is_numeric_dtype(joined[c])]
            guessed = guess_sales_column(joined)
            if numeric_cols:
                default_idx = numeric_cols.index(guessed) if guessed in numeric_cols else 0
                sales_col = st.selectbox("販売数の列を選択", numeric_cols, index=default_idx, key="sales_col_select")
                draw_sales_weather_chart(joined, sales_col)
            else:
                st.caption("販売数（数値）らしい列が見つかりませんでした。CSVの列名をご確認ください。")


if __name__ == "__main__":
    main()
