"""
TBM Analysis Report Generation
==============================

Scripts for generating TBM Analysis Reports from Fieldwire data dumps.

Usage:
    python -m scripts.tbm_analysis.generate_tbm_report --date 12-19-25 --contractors "Berg,MK Marlow"
    python -m scripts.tbm_analysis.generate_docx --input report_data.json

Process:
    1. Parse Fieldwire CSV data dump
    2. Filter by date and contractors
    3. Calculate metrics and identify observations
    4. Generate narratives using Claude Code (non-interactive)
    5. Produce styled DOCX report with MXI branding

See PROCESS.md for detailed documentation.
"""
