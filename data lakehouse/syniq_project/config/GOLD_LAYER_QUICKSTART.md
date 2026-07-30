# Gold Layer - Quick Start Guide

## What is the Gold Layer?

The Gold Layer is the **final, business-ready stage** of your data lakehouse. It performs:
- ✅ **Comprehensive EDA** (Exploratory Data Analysis)
- 📊 **Quality Assessment** (0-100 scores)
- 💡 **Automated Insights** 
- 📈 **Business Metrics**
- 📄 **Beautiful Reports** (HTML, JSON, MD)

## Quick Commands

### 1. List All Tables
```bash
python main.py gold list
```
Shows all tables from Silver layer ready for analysis.

### 2. Process Everything
```bash
python main.py gold process
```
Analyzes ALL tables, generates reports, creates Gold datasets.

### 3. Process One Table
```bash
python main.py gold process --source postgres --entity hosp_raya_patient_record
```
Focuses on a single table.

### 4. View Report
```bash
python main.py gold report postgres hosp_raya_patient_record
```
Quick summary in terminal.

### 5. Quick Summary
```bash
python main.py gold summary postgres hosp_raya_patient_record --sample
```
Fast overview with sample data.

## Understanding the Output

### Quality Scores 🎯
- **🟢 90-100**: Excellent - Production Ready
- **🟡 75-89**: Good - Minor Issues
- **🟠 60-74**: Fair - Needs Attention
- **🔴 <60**: Poor - Significant Issues

### What Gets Analyzed

#### For Every Column:
- Data type
- Missing values %
- Unique values
- Type-specific stats

#### Numeric Columns:
- Min, Max, Mean, Median
- Standard Deviation, Variance
- Quartiles (Q25, Q75)
- Skewness, Kurtosis
- Outlier detection

#### Categorical Columns:
- Most/least frequent values
- Top 10 values
- Text length statistics

#### DateTime Columns:
- Date ranges
- Temporal patterns

### Reports Generated

#### 📄 JSON Report
Machine-readable, complete analysis data
```
gold/eda_reports/postgres_table_eda_TIMESTAMP.json
```

#### 🌐 HTML Report
Beautiful, interactive web page
```
gold/eda_reports/postgres_table_eda_TIMESTAMP.html
```
**Open in browser for best experience!**

#### 📝 Markdown Report
Documentation-friendly format
```
gold/eda_reports/postgres_table_eda_TIMESTAMP.md
```

### Gold Datasets

Clean, enriched datasets saved as Parquet:
```
gold/postgres/table_name/table_name_gold_TIMESTAMP.parquet
```

## Typical Workflow

```bash
# Step 1: See what's available
python main.py gold list

# Step 2: Process all tables
python main.py gold process

# Step 3: Review reports
python main.py gold report postgres hosp_raya_patient_record

# Step 4: Open HTML reports
# Navigate to: gold/eda_reports/
# Open the .html files in your browser

# Step 5: Use Gold data
# Read from: gold/postgres/table_name/*.parquet
```

## Advanced Options

### Skip Gold Dataset Creation
```bash
python main.py gold process --no-save
```
Only generates reports, no data output.

### Skip Report Generation
```bash
python main.py gold process --no-report
```
Only creates datasets, no reports.

### Both Options
```bash
python main.py gold process --no-save --no-report
```
Only performs EDA, no outputs (for testing).

## What to Look For

### 🔍 In Reports

1. **Overall Quality Score**
   - Is it above 90? Great!
   - Below 75? Review insights

2. **Completeness**
   - High % of non-null values is good
   - Low completeness needs attention

3. **Key Insights**
   - Automated recommendations
   - Flags for anomalies
   - Data quality warnings

4. **Business Metrics**
   - Domain-specific calculations
   - Transaction totals, averages
   - Patient counts, age distributions

### 🚨 Red Flags

- Quality score < 60
- Completeness < 80%
- High duplicate percentage (>5%)
- Many outliers (>10% of data)
- Constant columns (no variation)

## Integration with Workflow

```
Bronze Layer (Raw)
     ↓
Silver Layer (Cleaned)
     ↓
Gold Layer (Analyzed) ← YOU ARE HERE
     ↓
Analytics/ML/BI Tools
```

## Tips & Best Practices

1. **Run regularly**: After each Silver update
2. **Monitor trends**: Track quality scores over time
3. **Act on insights**: Follow automated recommendations
4. **Use HTML reports**: Best visualization
5. **Archive reports**: Keep historical analysis

## Troubleshooting

### "No tables found"
- Check Silver layer has data: `ls -la silver/*/`
- Run Silver processing first

### "Out of memory"
- Process tables individually
- Use `--source` and `--entity` flags

### "Report not found"
- Run `gold process` first to generate reports
- Check `gold/eda_reports/` directory exists

## Next Steps

After Gold Layer:
1. **Analytics**: Use Gold datasets for BI dashboards
2. **Machine Learning**: Clean, profiled data ready for ML
3. **Business Intelligence**: Pre-calculated metrics available
4. **Data Quality**: Monitor quality scores over time

## Need Help?

- Full documentation: `GOLD_LAYER_README.md`
- Check logs: `logs/` directory
- View examples: `gold/eda_reports/` (after first run)

---
**Happy Analyzing! 📊**
