# 电影知识图谱分析

基于 PySpark 的 ETL 管线，对 [MovieLens](https://grouplens.org/datasets/movielens/) 数据集进行清洗，并导出结构化 CSV 文件，用于 **Neo4j** 知识图谱构建和 **Power BI** 可视化分析。

> **课程**：CSC 501 — 维多利亚大学

---

## 项目结构

```
Movie-Knowledge-Graph-Analysis/
├── data/
│   ├── raw/                    # 原始 MovieLens CSV（不纳入 git 追踪）
│   │   ├── movies.csv
│   │   ├── ratings.csv
│   │   └── tags.csv
│   └── manual fixes/
│       └──movieId_genre.json      # 手动修补缺失类型的 JSON 补丁
├── scripts/
│   ├── preprocessor.py         # 数据加载与清洗函数
│   ├── data_analysis.py        # 独立的分析报表函数
│   ├── io_utils.py             # CSV 导出工具（Neo4j / Power BI）
│   └── main.py                 # 管线入口脚本
├── output/                     # 运行时生成
│   ├── neo4j/                  # 6 个 CSV，供 Neo4j LOAD CSV 使用
│   ├── powerbi/                # 3 个宽表 CSV，供 Power BI 使用
│   └── exploration_log.txt     # 完整管线日志（含分析结果）
├── .gitignore
├── requirements.txt
├── README.md                   # 英文说明
└── README_zh.md                # 中文说明（本文件）
```

## 管线流程

```
原始 CSV ──▶ 清洗标准化 ──▶ 删除垃圾数据 ──▶ 手动修补 ──▶ 探索分析 ──▶ 导出
                │                 │               │             │           │
           类型转换          空年份 +         修正标题、     行数统计     Neo4j CSV
           去重             无类型 +          年份、类型     类型分布     Power BI CSV
           空值过滤          无标签           (来自 JSON)    热门电影     exploration_log
           年份提取           → 删除                         活跃用户
           类型拆分                                          热门标签
```

### 步骤详情

| 步骤 | 说明 |
|------|------|
| **加载与清洗** | 类型转换、字符串修剪、去重、过滤非法评分（范围 0.5–5.0）、从标题提取 `release_year`、将 `genres` 按 `\|` 拆分为多行、移除 IMAX 伪类型 |
| **删除垃圾数据** | 删除同时满足以下条件的电影：年份为空 + 类型为 `(no genres listed)` + 没有用户标签 |
| **手动修补** | 修正 6 部已知电影的标题/年份；使用 `manual_fixes.json` 替换 `(no genres listed)` 条目 |
| **探索分析** | 打印汇总统计：行数、去重数、类型分布、最多评分电影、最活跃用户、最高评分电影、热门标签 |
| **导出** | 将最终 CSV 写入 `output/neo4j/` 和 `output/powerbi/` |

所有探索输出同时打印到控制台并保存至 `output/exploration_log.txt`。

## 快速开始

### 环境要求

- **Python** 3.9+
- **Java** 8 / 11 / 17（PySpark 依赖）

### 1. 克隆与安装

```bash
git clone https://github.com/<your-username>/Movie-Knowledge-Graph-Analysis.git
cd Movie-Knowledge-Graph-Analysis
pip install -r requirements.txt
```

### 2. 添加数据

下载 [MovieLens Latest Small](https://grouplens.org/datasets/movielens/latest/) 数据集，将三个 CSV 文件放入 `data/raw/`：

```
data/raw/movies.csv
data/raw/ratings.csv
data/raw/tags.csv
```

### 3. 运行

```bash
cd scripts
python main.py
```

### 4. 查看结果

```
output/
├── neo4j/
│   ├── users_node.csv          # 用户节点
│   ├── movies_node.csv         # 电影节点
│   ├── genres_node.csv         # 类型节点
│   ├── ratings_rel.csv         # 用户→电影 评分关系
│   ├── movie_genre_rel.csv     # 电影→类型 关系
│   └── tags_rel.csv            # 用户→电影 标签关系
├── powerbi/
│   ├── movie_ratings_flat.csv  # 反规范化宽表（评分+电影+类型）
│   ├── movie_genre_stats.csv   # 按电影-类型聚合的评分统计
│   └── user_stats.csv          # 按用户聚合的评分统计
└── exploration_log.txt          ← 完整管线日志
```

## 模块说明

| 模块 | 职责 |
|------|------|
| `preprocessor.py` | `load_csv()`、`load_json()`、`clean_movies()`、`clean_ratings()`、`clean_tags()`、`explode_genres()` |
| `data_analysis.py` | `run_all()` — 独立分析报表（类型分布、热门电影、活跃用户等） |
| `io_utils.py` | `save_csv()`、`export_neo4j()`、`export_powerbi()` — 将 Spark 分区合并为单个 CSV |
| `main.py` | 管线总控 — 按顺序执行所有步骤，输出探索日志 |

## 数据来源

[MovieLens Latest Small](https://grouplens.org/datasets/movielens/latest/) — 600 位用户对 9,700 部电影的 100,000 条评分和 3,600 条标签。最后更新于 2018 年 9 月。

> F. Maxwell Harper and Joseph A. Konecny. 2015. The MovieLens Datasets: History and Context. *ACM Transactions on Interactive Intelligent Systems* 5, 4, Article 19.

## 许可

本项目仅供学术用途。
