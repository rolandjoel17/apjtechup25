# Databricks & Snowflake Hands-On Lab: E-Commerce Data Lakehouse

This comprehensive hands-on lab demonstrates building a complete medallion architecture (Bronze-Silver-Gold) using Databricks and Snowflake for an e-commerce platform. Designed specifically for Snowflake Solution Engineers to gain hands-on experience with Databricks while leveraging familiar SQL patterns.

## 🎯 Lab Overview

**Duration:** 3-4 hours  
**Audience:** Snowflake Solution Engineers  
**Data Volume:** ~10 million records  
**Complexity:** Intermediate to Advanced

## 🏗️ Architecture Overview

```
Raw Data (CSV) → Bronze (Delta) → Silver (Delta) → Gold (Iceberg)
                    ↓               ↓               ↓
                Raw Tables      Clean Tables   Analytics Tables
                ~10.1M rows     ~9.5M rows      ~50K rows
```

### Storage Formats by Layer
- **Bronze Layer**: Delta Lake tables with raw data ingestion (10 tables)
- **Silver Layer**: Delta Lake tables with cleaned and enriched data (7 tables)  
- **Gold Layer**: Apache Iceberg tables with business-ready analytics (4 tables)
- **ELT Processes**: SQL-based transformations demonstrating best practices

## 🛍️ Business Scenario: E-Commerce Platform

This lab simulates a realistic e-commerce platform with:
- **500K customers** with demographics and behavior data
- **100K products** across multiple categories and suppliers
- **2M orders** with complete transaction history
- **6M order line items** (largest table in the dataset)
- **Web analytics**, **inventory tracking**, and **shipping data**

### Key Business Questions Answered
- Who are our highest-value customers and what's their churn risk?
- Which products and categories drive the most revenue?
- What are our seasonal sales patterns and trends?
- Which inventory items need immediate attention?
- How do different customer segments behave?

## 📁 Project Structure

```
├── LAB_GUIDE.md              # Step-by-step lab instructions
├── SCHEMA_DESIGN.md          # Detailed schema documentation
├── data_generation/          # Python scripts for realistic data generation
│   ├── generate_data.py      # Main data generation script (~10M records)
│   └── requirements.txt      # Python dependencies
├── bronze/                   # Bronze layer (Raw data in Delta format)
│   ├── ddl/                  # Delta table DDL with optimization settings
│   └── sample_data/          # Generated CSV files (created by script)
├── silver/                   # Silver layer (Cleaned data in Delta format)
│   ├── ddl/                  # Enhanced table schemas with business logic
│   └── elt/                  # SQL transformations from bronze to silver
├── gold/                     # Gold layer (Analytics in Iceberg format)
│   ├── ddl/                  # Iceberg tables optimized for analytics
│   └── elt/                  # SQL aggregations from silver to gold
├── queries/                  # Comprehensive analytical queries
│   └── analytical_queries.sql # Business intelligence queries
└── setup/                    # Setup and configuration
    └── setup_instructions.md # Detailed setup guide
```

## 🚀 Quick Start

### Option 1: Full Lab Experience (Recommended)
Follow the complete step-by-step guide: **[LAB_GUIDE.md](LAB_GUIDE.md)**

### Option 2: Fast Track Setup
```bash
# 1. Generate data
cd data_generation && pip install -r requirements.txt && python generate_data.py

# 2. Upload CSVs to Databricks DBFS

# 3. Run SQL scripts in order:
# - bronze/ddl/*.sql
# - silver/ddl/*.sql  
# - silver/elt/*.sql
# - gold/ddl/*.sql
# - gold/elt/*.sql

# 4. Explore with queries/analytical_queries.sql
```

## 🎯 Lab Learning Objectives

By completing this lab, you will:

✅ **Understand Medallion Architecture**
- Implement Bronze-Silver-Gold data layers
- Apply data quality progression principles
- Design for different consumption patterns

✅ **Master Storage Formats**
- Work with Delta Lake for transactional data
- Implement Apache Iceberg for analytics
- Compare performance characteristics

✅ **Build ELT Pipelines**
- Create SQL-based transformation workflows
- Apply data cleaning and enrichment techniques
- Implement business logic in SQL

✅ **Develop Analytics Expertise**
- Design customer lifetime value models
- Build product performance analytics
- Create operational dashboards

✅ **Compare Platform Approaches**
- Understand Databricks vs Snowflake differences
- Adapt patterns between platforms
- Choose appropriate tools for use cases

## 📊 Data Volume & Performance

| Layer | Tables | Total Records | Largest Table | Purpose |
|-------|--------|---------------|---------------|---------|
| Bronze | 10 | ~10.1M | order_items_raw (6M) | Raw data ingestion |
| Silver | 7 | ~9.5M | order_items_clean (6M) | Cleaned data |
| Gold | 4 | ~50K | customer_metrics (500K) | Analytics |

### Expected Performance
- **Bronze queries**: < 2 minutes (full table scans)
- **Silver queries**: < 30 seconds (optimized with partitioning)
- **Gold queries**: < 5 seconds (pre-aggregated analytics)

## 🔧 Technical Features Demonstrated

### Delta Lake Features
- ACID transactions
- Time travel capabilities
- Automatic schema evolution
- Z-order optimization
- Auto-optimize and auto-compact

### Apache Iceberg Features
- Schema evolution
- Hidden partitioning
- Efficient metadata handling
- Cross-engine compatibility

### Advanced SQL Patterns
- Window functions for analytics
- CTEs for complex transformations
- Advanced aggregations
- Performance optimization techniques

## 🎓 Prerequisites

### Technical Requirements
- **Databricks**: Community Edition or higher
- **Python**: 3.8+ with pandas, numpy, faker
- **Storage**: ~5GB for generated data and tables
- **Time**: 3-4 hours for complete lab

### Knowledge Prerequisites
- SQL proficiency (intermediate to advanced)
- Basic understanding of data warehousing concepts
- Familiarity with Snowflake (helpful but not required)

## 📚 Additional Resources

### Documentation
- **[Setup Instructions](setup/setup_instructions.md)** - Detailed environment setup
- **[Schema Design](SCHEMA_DESIGN.md)** - Complete data model documentation
- **[Lab Guide](LAB_GUIDE.md)** - Step-by-step lab walkthrough

### Platform Documentation
- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Guide](https://docs.delta.io/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)

### Real-World Extensions
- Implement streaming data ingestion with Kafka
- Add machine learning pipelines with MLflow
- Set up automated data quality monitoring
- Create cross-cloud data sharing scenarios

## 🤝 Support

### Issues and Questions
- Review the [Setup Instructions](setup/setup_instructions.md) for troubleshooting
- Check the [Lab Guide](LAB_GUIDE.md) for step-by-step help
- Common issues and solutions are documented in setup files

### Contributing
This lab is designed for educational purposes. Contributions for improvements are welcome.

---

**Ready to start?** Begin with the **[LAB_GUIDE.md](LAB_GUIDE.md)** for a complete hands-on experience! 🚀
