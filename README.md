
# Weather_Demo_App
`ORG_ID` / `HOD_API_KEY` / `SAAS_CLIENT_ID` / `TENANT_ID` を .env にプリセット済み

## セットアップ

```bash
python3 -m venv .venv
source .venv/bin/activate  
pip install -r requirements.txt
```

## .env の内容

- HOD_API_KEY
- ORG_ID
- SAAS_CLIENT_ID（saascore-...）
- TENANT_ID（ハイフン無し接頭辞1）
- GEOSPATIAL_CLIENT_ID（未設定でも TENANT_ID から `geospatial-<TENANT_ID>` を自動入力する）
- START / END（既定: 2025-06-01 ～ 2025-06-30）

## 起動（GUI）

```bash
streamlit run app.py
```
> 403/401 が出る場合は `GEOSPATIAL_CLIENT_ID` を確認。

