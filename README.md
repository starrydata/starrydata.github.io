# starrydata.github.io

**Starrydata** — 論文グラフから抽出した実験データと、Materials Informatics 研究を支える Web ツールを公開するサイトのソースリポジトリです。

[starrydata.nims.go.jp](https://starrydata.nims.go.jp/) として公開されています（NIMS プロキシ経由）。

---

## データセット規模（2026-08 時点）

| 論文数 | 図数 | サンプル数 | カーブ数 |
|-------:|-----:|-----------:|---------:|
| 17,389 | 60,255 | 105,312 | 234,150 |

収録プロジェクト: Thermoelectric / Battery / Magnetic / Dielectric / Condensed Matter / Piezoelectric / Solid-State Battery / Air Battery / Organic Thermoelectric / Low/High Thermal Conductivity / Hypermaterial / General DB

データセット本体は [Figshare (Project #155129)](https://figshare.com/projects/Starrydata2/155129) で配布しています。

---

## リポジトリ構成

```
.
├── hp/                   # メイン Web サイト（/hp/ で配信）
│   ├── index.html        # トップページ（日本語）
│   ├── en/               # 英語ページ
│   ├── data/             # データ探索ページ（熱電・電池・磁性など）
│   ├── research/         # 研究紹介
│   ├── publications/     # 論文リスト
│   ├── news/             # ニュース
│   ├── members/          # メンバー
│   ├── contact/          # お問い合わせ
│   ├── common/           # 共通 CSS / JS
│   └── assets/           # ロゴ・画像
├── html/                 # 旧来の HTML ページ群
├── json/                 # サイトコンテンツ用データファイル
│   ├── counts.json       # データセット件数（自動更新）
│   ├── news.json / news_en.json
│   ├── people.json / people_en.json
│   ├── publications.json / publications_en.json
│   └── ...
├── scripts/
│   └── count_dataset.py  # Figshare からデータ件数を取得するスクリプト
├── images/               # サイト画像
├── links_archive/        # 旧リンク集アーカイブ
└── .github/workflows/
    └── update-counts.yml # 月次自動更新ワークフロー
```

---

## 自動化

### データ件数の自動更新

`.github/workflows/update-counts.yml` が毎月1日 06:00 JST に実行されます。

1. `scripts/count_dataset.py` が Figshare API から最新データセット (ZIP) を取得
2. 論文・図・サンプル・カーブをプロジェクト別に集計
3. 結果を `json/counts.json` に書き込み、自動コミット・プッシュ

手動実行は GitHub Actions の **Run workflow** ボタンから行えます。

---

## コンテンツの更新方法

| 更新対象 | 編集ファイル |
|----------|-------------|
| ニュース | `json/news.json` / `json/news_en.json` |
| メンバー | `json/people.json` / `json/people_en.json` |
| 論文リスト | `json/publications.json` / `json/publications_en.json` |
| 学会情報 | `json/conferences.json` / `json/conferences_en.json` |
| 研究内容 | `json/research.json` / `json/research_en.json` |
| データ件数 | 自動更新（手動の場合は Actions から実行） |

HTML / CSS / JS を変更した場合は、NIMS サーバーへの FTP アップロードも別途必要です。`json/` 以下のデータファイルは GitHub Pages から自動同期されます。

---

## ローカル確認

```bash
# 静的ファイルサーバーで手元確認（例: Python）
python -m http.server 8000
# → http://localhost:8000/hp/ を開く
```

---

## 関連リンク

- **公開サイト**: https://starrydata.nims.go.jp/
- **データセット (Figshare)**: https://figshare.com/projects/Starrydata2/155129
- **開発元**: 国立研究開発法人 物質・材料研究機構 (NIMS)

---

## ライセンス

データセットのライセンスは Figshare の各記事を参照してください。
サイトのコード・コンテンツに関するお問い合わせは [Contact](https://starrydata.nims.go.jp/hp/contact/) からどうぞ。
