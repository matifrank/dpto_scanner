name: Zonaprop → Google Sheet → Telegram

on:
  schedule:
    - cron: "15 11 * * *"  # 08:15 ART
    - cron: "15 21 * * *"  # 18:15 ART
  workflow_dispatch: {}

permissions:
  contents: read

jobs:
  run:
    runs-on: self-hosted
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Python info (self-hosted)
        run: |
          where python
          where py
          python --version
          py -3.11 --version

      - name: Install deps
        run: |
          py -3.11 -m pip install --upgrade pip
          py -3.11 -m pip install -r requirements.txt

      - name: Run scraper
        env:
          GCP_SA_JSON: ${{ secrets.GCP_SA_JSON }}
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          SHEET_ID: "1b189NF_lflAM0A0IfhI5xu_iGQdRzfcNGjqS7sAmBa4"

          MAX_USD: "121000"
          MAX_EXP: "120000"
          MIN_AMB: "2"
          ZONAS_OK: "olivos,villa urquiza,coghlan,colegiales,belgrano,vicente lopez,vicente lópez"

          MAX_NEW_URLS_PER_RUN: "120"
          SLEEP_SEC: "1.2"
        run: |
          py -3.11 scripts/zonaprop_to_sheets.py
