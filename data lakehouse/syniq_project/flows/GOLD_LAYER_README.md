# Gold Layer - Comprehensive EDA & Business-Ready Datasets

## Overview

The **Gold Layer** is the final, refined stage in the Syniq Data Lakehouse architecture. It provides:

- **Comprehensive Exploratory Data Analysis (EDA)** on all silver layer tables
- **Business-ready datasets** with enriched metadata
- **Automated insights** and data quality assessments
- **Multi-format reports** (JSON, HTML, Markdown)
- **Statistical analysis** and distribution profiling
- **Correlation detection** and outlier identification

## Architecture

```
Bronze Layer (Raw Data)
    ↓
Silver Layer (Cleaned, Validated)
    ↓
Gold Layer (EDA + Business Metrics)
    ↓
Analytics & ML Ready
```

## Features

### 1. **Automatic Table Discovery**
- Scans Silver layer for all available tables
- Lists tables by source system
- Provides metadata (row count, file info)

### 2. **Comprehensive EDA**
Each table undergoes thorough analysis:

#### Basic Information
- Row/column counts
- Memory usage
- Duplicate detection
- Data types profiling

#### Data Quality Assessment
- Completeness score (0-100)
- Missing value analysis
- Duplicate percentage
- Overall quality score

#### Column-Level Analysis
- **Numeric columns**: min, max, mean, median, std, variance, quartiles, skewness, kurtosis
- **Categorical columns**: value frequencies, most/least common, text length stats
- **Datetime columns**: date ranges, temporal distribution

#### Statistical Analysis
- Distribution identification (normal, skewed, etc.)
- Correlation matrices
- Strong correlation detection (>0.7)
- Histogram generation

#### Outlier Detection
- IQR-based outlier identification
- Outlier counts and percentages
- Boundary calculations

#### Pattern Recognition
- Constant columns
- Potential ID columns
- Low cardinality fields

#### Business Metrics
- Transaction-specific metrics (totals, averages)
- Patient/record metrics
- Custom domain calculations

#### Automated Insights
- Data quality recommendations
- Anomaly alerts
- Best practice suggestions

### 3. **Multi-Format Reporting**

#### JSON Report
- Machine-readable
- Complete analysis data
- API-friendly format

#### HTML Report
- Beautiful, styled interface
- Interactive visualizations
- Quality score badges
- Color-coded metrics

#### Markdown Report
- Documentation-friendly
- Version control compatible
- Human-readable tables

### 4. **Gold Datasets**
- Cleaned and enriched data
- Quality score metadata
- Processing timestamps
- Minimal technical overhead

## Commands

### List Available Tables
```bash
python main.py gold list
```
**Output**: All tables in Silver layer ready for Gold processing

### Process All Tables
```bash
python main.py gold process
```
**Actions**:
- Performs full EDA on all tables
- Generates reports (JSON, HTML, MD)
- Creates Gold datasets
- Produces master summary

### Process Specific Table
```bash
python main.py gold process --source postgres --entity hosp_raya_patient_record
```
**Actions**:
- Performs full EDA on one table
- Generates individual reports
- Creates Gold dataset

### Skip Report Generation (EDA only)
```bash
python main.py gold process --no-report
```

### Skip Gold Dataset Creation (Reports only)
```bash
python main.py gold process --no-save
```

### View EDA Report
```bash
python main.py gold report postgres hosp_raya_patient_record
```
**Output**: Summary of EDA findings, quality scores, insights

### Quick Table Summary
```bash
python main.py gold summary postgres hosp_raya_patient_record
```
**Output**: Quick stats without full processing

### View Sample Data
```bash
python main.py gold summary postgres hosp_raya_patient_record --sample
```

## Directory Structure

```
gold/
├── postgres/
│   └── hosp_raya_patient_record/
│       └── hosp_raya_patient_record_gold_20260223_140530.parquet
├── eda_reports/
│   ├── postgres_hosp_raya_patient_record_eda_20260223_140530.json
│   ├── postgres_hosp_raya_patient_record_eda_20260223_140530.html
│   ├── postgres_hosp_raya_patient_record_eda_20260223_140530.md
│   └── MASTER_REPORT.json
```

## Output Files

### Gold Datasets
**Location**: `gold/{source}/{table}/`
**Format**: Parquet
**Contents**: 
- All original columns from Silver
- `_gold_processed_at`: Processing timestamp
- `_quality_score`: Overall quality score
- Essential metadata from Bronze/Silver

### EDA Reports
**Location**: `gold/eda_reports/`

#### JSON (`*_eda_*.json`)
Complete analysis data structure

#### HTML (`*_eda_*.html`)
Features:
- Responsive design
- Gradient header
- Metric cards
- Quality score visualization
- Styled tables
- Correlation highlights
- Insights display

#### Markdown (`*_eda_*.md`)
Features:
- Formatted tables
- Section headers
- List-based insights
- Easy to version control

#### Master Report (`MASTER_REPORT.json`)
Summary of all processed tables

## Quality Scoring

### Overall Quality Score (0-100)
Calculated based on:
- Data completeness (% non-null values)
- Duplicate penalty (up to -20 points)

### Score Interpretation
- **90-100** 🟢 Excellent - Production ready
- **75-89** 🟡 Good - Minor issues
- **60-74** 🟠 Fair - Needs attention
- **<60** 🔴 Poor - Significant issues

## Use Cases

### 1. **Data Profiling**
Before analysis, understand your data structure, quality, and characteristics.

### 2. **Quality Monitoring**
Track data quality scores over time, identify degradation patterns.

### 3. **Feature Engineering**
Use correlation analysis and distributions to inform ML feature selection.

### 4. **Business Intelligence**
Business metrics provide instant KPIs without writing SQL.

### 5. **Data Documentation**
Auto-generated reports serve as data dictionaries.

### 6. **Anomaly Detection**
Automated insights flag unusual patterns and outliers.

## Integration with Other Layers

### From Silver Layer
- Reads validated, cleaned data
- Inherits quality gate results
- Accesses transformation metadata

### To Analytics/ML
- Provides ready-to-use datasets
- Includes feature statistics for ML
- Offers quality scores for filtering

## Best Practices

1. **Regular Processing**: Run Gold layer after each Silver update
2. **Monitor Quality Scores**: Track trends over time
3. **Review Insights**: Act on automated recommendations
4. **Archive Reports**: Keep historical EDA for auditing
5. **Customize Metrics**: Extend business_metrics for domain-specific KPIs

## Example Workflow

```bash
# 1. Discover available tables
python main.py gold list

# 2. Process all tables with full EDA
python main.py gold process

# 3. Review specific table report
python main.py gold report postgres hosp_raya_patient_record

# 4. Check quality scores
grep "quality_score" gold/eda_reports/MASTER_REPORT.json

# 5. Open HTML report in browser
start gold/eda_reports/postgres_hosp_raya_patient_record_eda_*.html
```

## Technical Details

### Dependencies
- pandas: Data manipulation
- numpy: Numerical operations
- scipy: Statistical tests
- pyarrow: Parquet I/O

### Performance
- Handles large datasets efficiently
- Parallel processing capable (future)
- Incremental updates supported (future)

### Extensibility
Easily extend with:
- Custom business metrics
- Additional visualizations
- Domain-specific analyses
- Integration with BI tools

## Troubleshooting

### No tables found
```bash
# Check Silver layer has data
ls -la silver/*/
```

### Out of memory
```bash
# Process tables individually
python main.py gold process -s postgres -e table_name
```

### Report not generated
```bash
# Check logs in logs/ directory
# Ensure write permissions in gold/eda_reports/
```

## Future Enhancements

- [ ] Interactive dashboards
- [ ] Real-time EDA streaming
- [ ] ML-based anomaly detection
- [ ] Automated remediation suggestions
- [ ] Data drift detection
- [ ] Custom report templates
- [ ] Export to Excel/PDF
- [ ] Integration with data catalogs

---

**Generated by Syniq Data Lakehouse**  
© 2026 All Rights Reserved
