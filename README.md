# Overdose vs Poverty Analysis (Spark)

## Overview
This project investigates whether counties with higher poverty rates experience higher overdose emergency department (ED) visit rates.

## Datasets Used
- Virginia Department of Health (VDH) Overdose ED Visits dataset
- US Census ACS dataset

## Methodology
- Data cleaning using Apache Spark
- Created poverty_rate variable
- Joined datasets at county level
- Grouped analysis by poverty levels

## Results
- Low poverty areas had lower overdose rates (~42)
- Medium and high poverty areas had higher rates (~60)

## Tools Used
- Apache Spark (Scala)

## Notes
This project uses a model-free analysis approach.