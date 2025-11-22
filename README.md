# ETL Pipeline and Market Analysis Project

## Overview
Develop a comprehensive ETL pipeline and perform in-depth market analysis on a fictional stock dataset. The goal is to move raw CSV files into a clean, queryable SQL schema, normalizing/denormalizing the data as appropriate for analysis. Also, answer key questions about user behavior and generate visualizations.

---

## Features

### EDA
- Identify and document problems with the data provided.

### ETL Pipeline
- Build an ETL pipeline that cleans the data and loads it into your data warehouse.

### Data Warehouse
- Design a schema for your data warehouse that prioritizes efficient storage.

### Data Marts
- Create new tables for your Data Marts suitable for the analysis and visualization prompts that follow.
- Organize your data marts to prioritize query speed.

### Analysis
- Document answers to the following questions in code sections and display supporting data:
  - #### Portfolios
    - Find the 5 highest valued portfolios and 5 lowest valued portfolios.
    - What portfolios performed the best across the year?
    - What tickers had the highest value trades?
    - Which tickers contributed most to portfolio value growth across all accounts?
    - Which portfolios had the most consistent growth across the year (lowest volatility)?
    - Identify portfolios that had extreme swings in value month-over-month.
    - Define a KPI not already addressed, explain its importance, and identify which portfolios are most and least successful using that metric.

  - #### Transactions
    - Identify any instances where many accounts all made similar transactions on the same day.
    - Identify accounts that hold high-risk portfolios (high volatility tickers) but trade very frequently.
    - Identify any instances of potential insider trading.

  - #### Market Risks
    - Calculate ticker volatility for the year.
    - Identify accounts whose total holdings are relatively large compared to average trading volume.
    
### Visualizations
  - Include `.pbit` files and instructions in the README on how to connect to the database.
  - Plot distributions of trading volume by ticker over time.
  - Create a line chart showing portfolio value over time for the 5 highest-valued and 5 lowest-valued portfolios to visualize growth trajectories.
  - Visualize portfolio volatility.
  - Provide visualizations to examine individual portfolio performance at quarterly, monthly, and daily levels.
  - Compare portfolio performance to overall market performance.
  - Compare retirement account performance with non-retirement account performance.

### Documentation
  - README explaining the purpose and process for setting up the project.
  - Detail any design decisions and include your EDA results.
  - Include relevant diagrams.
  - Provide any additional analysis or data as appropriate.

---

## Requirements

- All code shall be written in Jupyter Notebooks and SQL. Organize the files to contain your ETL pipeline in one, your data mart creations in another, and your analysis in a third.

- The database used shall be Postgres.

- git requirements

  - Any raw data shall files be ignored by git

  - Follow GitHub best practices.

    - Small commits with related changes. 
    - Meaningful commit messages. 
    - Push commits to the remote at least once a day

- The complete process for the above should be runnable from scratch in your jupyter notebooks. i.e., with just the data, everything should be reproducible by running the code from the beginning.

- Maintain a docker compose file and ensure that you are not storing any of the table data in the GitHub repository.

- Include any documentation, planning, or diagrams in the repository.

- Name your folder holding the CSVs ‘data’ and have that folder exist in your parent directory for the project. Your code must not edit the CSVs in this data folder.