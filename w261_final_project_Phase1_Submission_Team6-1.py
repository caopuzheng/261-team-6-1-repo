# Databricks notebook source
# MAGIC %md
# MAGIC # Instructions:
# MAGIC
# MAGIC Objectives:
# MAGIC - Explain data (i.e.., simple exploratory analysis of various fields, such as the semantic as well the intrinsic meaning of ranges, null values, categorical/numerical, mean/std.dev to normalize and/or scale inputs). Identify any missing or corrupt (i.e., outlier) data.
# MAGIC - Define the outcome (i.e., the evaluation metric and the target) precisely, including mathematical formulas.
# MAGIC - How do you ingest the CSV files and represent them efficiently? (think about different file formats)
# MAGIC - Summarized relevant datasets based on three months of the joined On Time Performance and Weather  (OTPW data); see Azure storage bucket via the Databricks starting notebook
# MAGIC - Split the data train/validation/test - making sure no leaks occur, for example: normalize using the training statistics. 

# COMMAND ----------

# MAGIC %md
# MAGIC # Project Title

# COMMAND ----------

# MAGIC %md
# MAGIC # Team Members
# MAGIC
# MAGIC | Name     | Email | Photo???? | 
# MAGIC | --------- | --------- | --------- | 
# MAGIC | Juliana Gómez Consuegra  | julianagc@berkeley.edu | |
# MAGIC | Ray Cao | caopuzheng@berkeley.edu | |
# MAGIC | Jenna Sparks | jenna_sparks@berkeley.edu | |
# MAGIC | Dhyuti Ramadas | dramadas@berkeley.edu | |
# MAGIC | Rachel Gao | jgao1008@berkeley.edu | |

# COMMAND ----------

# MAGIC %md
# MAGIC # Phase Leader Plan
# MAGIC
# MAGIC | Phase             | Week | Dates       | Leader                  | Github         |
# MAGIC | ----------------- | ---- | ----------- | ----------------------- | -------------- |
# MAGIC | Phase 1           | 0    | 3/11 - 3/17 | All                     |                |
# MAGIC | Phase 2           | 1    | 3/18 - 3/24 | Juliana Gómez Consuegra | @JulianaGomez  |
# MAGIC | Phase 2           | 2    | 3/25 - 3/31 | Ray Cao                 | @caopuzheng    |
# MAGIC | Phase 2 & Phase 3 | 3    | 4/1 - 4/7   | Jenna Sparks            | @jennasparks   |
# MAGIC | Phase 3           | 4    | 4/8 - 4/14  | Dhyuti Ramadas          | @dramadas      |
# MAGIC | Phase 3           | 5    | 4/15 - 4/21 | Rachel Gao              | @rachelgaoMIDS |
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Credit assignment plan 
# MAGIC
# MAGIC https://github.com/users/caopuzheng/projects/4/views/5

# COMMAND ----------

# MAGIC %md
# MAGIC # Abstract
# MAGIC
# MAGIC - Around 150-200 words
# MAGIC - In your own words, summarize at a high level the project, the data, your planned pipelines/experiments, metrics, etc.
# MAGIC - https://www.aje.com/arc/make-great-first-impression-6-tips-writing-strong-abstract/

# COMMAND ----------

# MAGIC %md
# MAGIC # Data description
# MAGIC
# MAGIC - Basic analysis and understanding of data
# MAGIC - Include:
# MAGIC   - What data you plan to use
# MAGIC   - Some summary visual EDA

# COMMAND ----------

# MAGIC %md
# MAGIC # Machine algorithms and metrics 
# MAGIC
# MAGIC - about 200-300 words
# MAGIC ## Description of algorithms to be used
# MAGIC
# MAGIC - Baseline: Logistic Regression
# MAGIC - Random Forest
# MAGIC - Might consider regression approach (if time permitted) 
# MAGIC   - benefit of predicting actual delay time
# MAGIC - Might consider Deep learning network (if time permitted)
# MAGIC
# MAGIC
# MAGIC ## implementations
# MAGIC ## loss functions
# MAGIC   - why
# MAGIC ## Description of metrics and analysis to be used
# MAGIC   - include the equations for the metrics
# MAGIC   - include both standard metrics and domain-specific metrics
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Machine Learning Pipelines
# MAGIC
# MAGIC - Description of pipeline steps (and a block/Gantt diagram) involved in completing this task and a timeline

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion and next steps

# COMMAND ----------

# MAGIC %md
# MAGIC # Open issues or problems
