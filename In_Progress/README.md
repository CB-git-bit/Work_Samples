# In_Progress Directory
*In progress files for Work Sample project* 

## Introduction
This directory contains unfinished code that still needs to be integrated into the main pipeline. 
Additional next steps that I would like to integrate are included.

## Requirements
For project requirements see the Work_Samples README.md

## Directory Structure
This folder contains the following files:
- `vol_avg_unittest.py`: untested code for conducting unit tests on the `calculate_vol_moving_avg()` function
- `adj_close_med_unittest.py`: untested code for conducting unit tests on the `calculate_adj_close_rolling_med()` function

## Tasks
The following tasks are items I plan to continue to integrate into this project:
- Improve, modify, and test the training model, `MLmodel_v2.py`, the DAG, `my_dag.py`, and the model service `pipeline_app.py`.
  Limited system capabilities has been a significant setback in developing these steps. 
- Incorporate unit tests, logic tests, and other fail safes to ensure program robustness
- BONUS step from Problem 4: Test the API service, document your methodology, provisioned computing resources, test results, a breakdown of observable bottlenecks (e.g. model loading/inference, socket/IO, etc.), and improvement suggestions for hypothetical future iterations.
- Incorporate visualizations to the API service of the results 
- Analyze best parameter results from training the ML model to ensure model isn't overfitted
- Integrate a database to host the contest instead of locally to improve process time


