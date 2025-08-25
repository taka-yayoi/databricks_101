# Databricks notebook source
# MAGIC %md
# MAGIC # はじめてのDatabricks
# MAGIC
# MAGIC このノートブックではDatabricksの基本的な使い方をご説明します。
# MAGIC
# MAGIC [はじめてのDatabricks](https://qiita.com/taka_yayoi/items/8dc72d083edb879a5e5d)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricksの使い方
# MAGIC
# MAGIC ![how_to_use.png](./how_to_use.png "how_to_use.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 画面の説明
# MAGIC
# MAGIC Databricksでデータ分析を行う際に頻繁に使用するのが、今画面に表示している**ノートブック**です。AIアシスタントをはじめ、分析者の生産性を高めるための工夫が随所に施されています。
# MAGIC
# MAGIC - [日本語設定](https://qiita.com/taka_yayoi/items/ff4127e0d632f5e02603)
# MAGIC - [ワークスペース](https://docs.databricks.com/ja/workspace/index.html)
# MAGIC - [ノートブック](https://docs.databricks.com/ja/notebooks/index.html)の作成

# COMMAND ----------

# MAGIC %md
# MAGIC ## 計算資源(コンピュート)
# MAGIC
# MAGIC Databricksにおける計算資源は[コンピュート](https://docs.databricks.com/ja/compute/index.html)と呼ばれます。Databricksが全てを管理する**サーバレス**の計算資源も利用できます。
# MAGIC
# MAGIC - コンピュート
# MAGIC - SQLウェアハウス

# COMMAND ----------

print("Hello Databricks!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの読み込み
# MAGIC
# MAGIC Databricksでは上述のコンピュートを用いて、ファイルやテーブルにアクセスします。SQLやPython、Rなどを活用することができます。Databricksノートブックではこれらの[複数の言語を混在](https://docs.databricks.com/ja/notebooks/notebooks-code.html#mix-languages)させることができます。
# MAGIC
# MAGIC ![how_to_use2.png](./how_to_use2.png "how_to_use2.png")
# MAGIC
# MAGIC - [ビジュアライゼーション/データプロファイル](https://docs.databricks.com/ja/visualizations/index.html)
# MAGIC - [AIアシスタント](https://docs.databricks.com/ja/notebooks/databricks-assistant-faq.html)
# MAGIC
# MAGIC AIアシスタントを活用してロジックを組み立てることができます。アシスタントを呼び出すには![assistant.png](./assistant.png "assistant.png")アイコンをクリックします。
# MAGIC
# MAGIC プロンプト: `samples.tpch.ordersの中身を1000行表示`

# COMMAND ----------

orders_df = spark.table("samples.tpch.orders")
display(orders_df.limit(1000))

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの加工
# MAGIC
# MAGIC AIアシスタントを活用してロジックを組み立てることができます。なお、アシスタントを活用する際には**具体的に**指示することが重要です。
# MAGIC
# MAGIC プロンプト:`クエリーの処理に関する日本語コメントを追加して`
# MAGIC
# MAGIC **参考資料**
# MAGIC
# MAGIC - [Databricksアシスタントの新機能を試す](https://qiita.com/taka_yayoi/items/058f324960cecceef218)
# MAGIC - [Databricksアシスタントを用いたEDA\(探索的データ分析\)](https://qiita.com/taka_yayoi/items/07c49c2de588a101b719)
# MAGIC - [DatabricksアシスタントによるEDA\(探索的データ分析\) その2](https://qiita.com/taka_yayoi/items/29af5ef10aeba01ca391)
# MAGIC - [Databricksアシスタントによるデータエンジニアリング](https://qiita.com/taka_yayoi/items/b76ca7f8f11dce2be1c3)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1998年7月1日の注文を選択し、合計金額が100,000以上の注文をフィルタリング
# MAGIC SELECT
# MAGIC   o_orderkey,         
# MAGIC   o_custkey,          
# MAGIC   o_orderstatus,      
# MAGIC   o_totalprice,       
# MAGIC   o_orderdate,        
# MAGIC   o_orderpriority     
# MAGIC FROM
# MAGIC   samples.tpch.orders
# MAGIC WHERE
# MAGIC   o_orderdate = "1998-07-01"    
# MAGIC   AND o_totalprice >= 100000

# COMMAND ----------

# MAGIC %md
# MAGIC ## データの書き込み
# MAGIC
# MAGIC テーブルから別のテーブルを作成するといった作業をしていると、`このテーブルはどうやって作ったんだっけ？`となりがちです。そのような依存関係は、Databricksでは **リネージ(系統情報)** として自動で記録されます。
# MAGIC
# MAGIC - [リネージ](https://docs.databricks.com/ja/data-governance/unity-catalog/data-lineage.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE takaakiyayoi_catalog.databricks_101.tpch_orders_199807 AS
# MAGIC SELECT
# MAGIC   o_orderkey,
# MAGIC   o_custkey,
# MAGIC   o_orderstatus,
# MAGIC   o_totalprice,
# MAGIC   o_orderdate,
# MAGIC   o_orderpriority
# MAGIC FROM
# MAGIC   samples.tpch.orders
# MAGIC WHERE
# MAGIC   o_orderdate = "1998-07-01"
# MAGIC   AND o_totalprice >= 100000

# COMMAND ----------

# MAGIC %md
# MAGIC 上で作成したテーブルから、さらに別のテーブルを作成します。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE takaakiyayoi_catalog.databricks_101.tpch_orders_199807_derived AS
# MAGIC SELECT
# MAGIC   COUNT(o_orderpriority) AS order_cnt,
# MAGIC   o_orderpriority
# MAGIC FROM
# MAGIC   takaakiyayoi_catalog.databricks_101.tpch_orders_199807
# MAGIC GROUP BY
# MAGIC   o_orderpriority
# MAGIC ORDER BY
# MAGIC   order_cnt DESC

# COMMAND ----------

# MAGIC %md
# MAGIC [カタログエクスプローラ](https://e2-demo-west.cloud.databricks.com/explore/data/users/takaaki_yayoi?o=2556758628403379)でテーブルを確認しましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ## クリーンアップ

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE takaakiyayoi_catalog.databricks_101.tpch_orders_199807_derived;
# MAGIC DROP TABLE takaakiyayoi_catalog.databricks_101.tpch_orders_199807;

# COMMAND ----------

# MAGIC %md
# MAGIC **参考資料**
# MAGIC
# MAGIC - [Databricksドキュメント \| Databricks on AWS](https://docs.databricks.com/ja/index.html)
# MAGIC - [はじめてのDatabricks](https://qiita.com/taka_yayoi/items/8dc72d083edb879a5e5d)
# MAGIC - [Databricksチュートリアル](https://qiita.com/taka_yayoi/items/4603091dd325c77d577f)
# MAGIC - [Databricks記事のまとめページ\(その1\)](https://qiita.com/taka_yayoi/items/c6907e2b861cb1070f4d)
# MAGIC - [Databricks記事のまとめページ\(その2\)](https://qiita.com/taka_yayoi/items/68fc3d67880d2dcb32bb)
