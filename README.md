# 日本語版Wikipediaデータセットプロジェクト
このプロジェクトは、日本語版Wikipediaのダンプファイルを読み込み、クリーニングと整形を行った後、Hugging Faceのデータセットライブラリに登録するものです。
成果物はこちら
https://huggingface.co/datasets/tet550/jawiki_sentences

## ファイル構成
- jawiki_cleaning.py: 日本語版Wikipediaのダンプデータをクリーニングと整形するスクリプト
- jawiki_sentences.py: 整形したデータをHugging Faceのデータセットライブラリに登録するスクリプト
- require.txt: プロジェクトで必要なPythonパッケージをリストアップしたファイル

## 実行方法
- データセットの元となる日本語版Wikipediaのダンプファイルは事前にダウンロードし、下記パスに配置してください。
data_raw/jawiki-latest-pages-articles.xml.bz2
- 日本語版Wikipediaのダンプファイルは無償で公開されています（すごい）。
https://dumps.wikimedia.org/jawiki/latest/
- jawiki_cleaning.py スクリプトを実行して、日本語版Wikipediaのダンプデータのクリーニングと整形を行います。
- 下記ページの手順でhugginfaceに登録を行なってください
https://huggingface.co/docs/datasets/upload_dataset
- jawiki_sentences.py スクリプトを実行して、整形したデータをHugging Faceのデータセットライブラリに登録します
スクリプトの最後で、データセットの登録を行います。必要に応じてアップロード先を適宜変更してください。

## 注意事項
- ウィキペディアのコンテンツは Creative Commons Attribution-ShareAlike 4.0 International License (CC BY-SA 4.0) および GNU Free Documentation License (GFDL) の下にライセンスされています。
