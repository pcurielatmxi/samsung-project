#!/usr/bin/env python3
"""
TBM Analysis DOCX Generator
===========================

Generates styled DOCX reports with MXI branding from processed report content.
Input: JSON file from tbm_data_processor.py (with optional narratives)
Output: Professionally styled Word document

Usage:
    python docx_generator.py --input report_content.json --output report.docx
"""

import argparse
import json
from datetime import datetime
from pathlib import Path

from docx import Document
from docx.shared import Inches, Pt, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement


# =============================================================================
# MXI BRAND COLORS
# =============================================================================

class Colors:
    PRIMARY_BLUE = RGBColor(30, 58, 95)      # Headers, titles
    SECONDARY_BLUE = RGBColor(46, 89, 132)   # Subheaders
    ACCENT_BLUE = RGBColor(74, 144, 217)     # Category headers
    CRITICAL_RED = RGBColor(196, 30, 58)     # Alerts, idle
    WARNING_ORANGE = RGBColor(230, 126, 34)  # Warnings
    SUCCESS_GREEN = RGBColor(39, 174, 96)    # Good metrics
    DARK_GRAY = RGBColor(44, 62, 80)         # Body text
    WHITE = RGBColor(255, 255, 255)


# =============================================================================
# STYLING HELPERS
# =============================================================================

def set_spacing(paragraph, before=0, after=0, line=1.0):
    """Set paragraph spacing."""
    pf = paragraph.paragraph_format
    pf.space_before = Pt(before)
    pf.space_after = Pt(after)
    pf.line_spacing = line


def add_cell_shading(cell, hex_color: str):
    """Add background color to table cell."""
    shading = OxmlElement('w:shd')
    shading.set(qn('w:fill'), hex_color)
    cell._element.get_or_add_tcPr().append(shading)


def add_cell_padding(cell, padding: int = 100):
    """Add padding to table cell."""
    tc_pr = cell._element.get_or_add_tcPr()
    tc_mar = OxmlElement('w:tcMar')
    for side in ['top', 'bottom', 'left', 'right']:
        margin = OxmlElement(f'w:{side}')
        margin.set(qn('w:w'), str(padding))
        margin.set(qn('w:type'), 'dxa')
        tc_mar.append(margin)
    tc_pr.append(tc_mar)


# =============================================================================
# DOCUMENT BUILDER
# =============================================================================

class TBMReportBuilder:
    """Builds the TBM Analysis Report DOCX."""

    def __init__(self, content: dict):
        self.content = content
        self.doc = Document()
        self._setup_document()

    def _setup_document(self):
        """Configure document margins."""
        for section in self.doc.sections:
            section.top_margin = Inches(0.5)
            section.bottom_margin = Inches(0.5)
            section.left_margin = Inches(0.75)
            section.right_margin = Inches(0.75)

    def build(self, output_path: str):
        """Build complete report."""
        self._add_cover_page()
        self.doc.add_page_break()

        self._add_executive_dashboard()
        self.doc.add_page_break()

        self._add_category_1()
        self._add_category_2()
        self._add_category_3()
        self._add_category_4()
        self._add_category_5()
        self._add_category_6()
        self._add_category_7()
        self._add_category_8()

        self._add_end_marker()

        self.doc.save(output_path)

    # =========================================================================
    # COVER PAGE
    # =========================================================================

    def _add_cover_page(self):
        """Build cover page."""
        # Spacer
        p = self.doc.add_paragraph()
        set_spacing(p, before=72)

        # MXI
        p = self.doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run('MXI')
        run.font.size = Pt(48)
        run.font.bold = True
        run.font.color.rgb = Colors.PRIMARY_BLUE

        # Tagline
        p = self.doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run('Document Control • Project Management • Field Services')
        run.font.size = Pt(10)
        run.font.color.rgb = Colors.DARK_GRAY

        # Spacer
        self.doc.add_paragraph()

        # Title
        p = self.doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run('TBM ANALYSIS REPORT')
        run.font.size = Pt(24)
        run.font.bold = True
        run.font.color.rgb = Colors.PRIMARY_BLUE

        # Subtitle
        p = self.doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run(f"{self.content['contractor_group']} Subcontractor Submittals")
        run.font.size = Pt(14)

        self.doc.add_paragraph()

        # Project info
        p = self.doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run('SAMSUNG TAYLOR FAB - SECAI')
        run.font.size = Pt(13)
        run.font.bold = True

        p = self.doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run('1530 FM973, Taylor, TX 76574')
        run.font.size = Pt(11)

        self.doc.add_paragraph()

        # Summary stats table
        table = self.doc.add_table(rows=2, cols=4)
        table.alignment = WD_ALIGN_PARAGRAPH.CENTER

        headers = ['Total Tasks', 'Contractors', 'Floors', 'Report Pages']
        values = [
            str(self.content['total_tasks']),
            str(self.content['total_contractors']),
            str(self.content['total_floors']),
            '—'
        ]

        for i, (h, v) in enumerate(zip(headers, values)):
            # Header
            cell = table.rows[0].cells[i]
            cell.text = h
            cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
            cell.paragraphs[0].runs[0].font.bold = True
            cell.paragraphs[0].runs[0].font.size = Pt(9)
            add_cell_shading(cell, 'F5F5F5')

            # Value
            cell = table.rows[1].cells[i]
            cell.text = v
            cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
            cell.paragraphs[0].runs[0].font.size = Pt(14)
            cell.paragraphs[0].runs[0].font.bold = True

        self.doc.add_paragraph()

        # Date
        p = self.doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run(self.content['report_date_formatted'])
        run.font.size = Pt(13)
        run.font.bold = True

        # Spacer
        for _ in range(3):
            self.doc.add_paragraph()

        # Boots to Bytes
        p = self.doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run('Boots to Bytes')
        run.font.size = Pt(10)
        run.font.italic = True

        self.doc.add_paragraph()

        # Confidentiality
        p = self.doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run('CONFIDENTIAL - FOR INTERNAL USE ONLY')
        run.font.size = Pt(9)
        run.font.bold = True
        run.font.color.rgb = Colors.CRITICAL_RED

    # =========================================================================
    # EXECUTIVE DASHBOARD
    # =========================================================================

    def _add_executive_dashboard(self):
        """Build executive dashboard."""
        p = self.doc.add_paragraph()
        run = p.add_run('Executive Dashboard')
        run.font.size = Pt(16)
        run.font.bold = True
        run.font.color.rgb = Colors.PRIMARY_BLUE

        self.doc.add_paragraph()

        # KPI cards (2 rows × 3 cols)
        self._add_kpi_cards()

        self.doc.add_paragraph()

        # TBM Summary
        self._add_tbm_summary_section()

    def _add_kpi_cards(self):
        """Build KPI cards table."""
        table = self.doc.add_table(rows=4, cols=3)

        # Row 0: Headers
        cards = [
            ('Labor Productivity Index (LPI)', f"{self.content['lpi']:.0f}%", f"Target: {self.content['lpi_target']}%"),
            ('Direct-Indirect Labor Ratio', f"{self.content['dilr']:.0f}%", f"Target: <{self.content['dilr_target']}%"),
            ('Idle Time Hours', f"{self.content['idle_time_hours']:.0f}", f"@ ${self.content['labor_rate']}/hr = ${self.content['idle_time_cost']:,.0f}"),
        ]

        for i, (title, value, note) in enumerate(cards):
            # Header
            cell = table.rows[0].cells[i]
            cell.text = title
            cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
            cell.paragraphs[0].runs[0].font.size = Pt(9)
            cell.paragraphs[0].runs[0].font.bold = True
            add_cell_shading(cell, 'F5F5F5')

            # Value
            cell = table.rows[1].cells[i]
            cell.text = value
            cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
            run = cell.paragraphs[0].runs[0]
            run.font.size = Pt(24)
            run.font.bold = True
            run.font.color.rgb = Colors.CRITICAL_RED if i != 1 else Colors.WARNING_ORANGE

            # Note
            cell = table.rows[2].cells[i]
            cell.text = note
            cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.CENTER
            cell.paragraphs[0].runs[0].font.size = Pt(8)

        # Row 3: Second set of KPIs (empty spacer for now, could add more)

    def _add_tbm_summary_section(self):
        """Build TBM Summary table."""
        p = self.doc.add_paragraph()
        run = p.add_run('TBM Summary')
        run.font.size = Pt(12)
        run.font.bold = True
        run.font.color.rgb = Colors.SECONDARY_BLUE

        metrics = self.content['contractor_metrics']
        table = self.doc.add_table(rows=len(metrics) + 2, cols=5)

        # Headers
        headers = ['Contractor', 'Planned MP', 'TBM Presence', 'Actual', 'Accuracy']
        for i, h in enumerate(headers):
            cell = table.rows[0].cells[i]
            cell.text = h
            cell.paragraphs[0].runs[0].font.bold = True
            cell.paragraphs[0].runs[0].font.size = Pt(10)

        # Data rows
        for idx, m in enumerate(metrics, 1):
            table.rows[idx].cells[0].text = m['name']
            table.rows[idx].cells[1].text = f"{m['total_planned']:.0f}"
            table.rows[idx].cells[2].text = f"{m['tbm_presence']:.0f}"
            table.rows[idx].cells[3].text = f"{m['total_verified']:.0f}"
            table.rows[idx].cells[4].text = f"{m['accuracy']:.0f}%"

            # Color code accuracy
            acc_cell = table.rows[idx].cells[4]
            acc = m['accuracy']
            if acc < 40:
                add_cell_shading(acc_cell, 'FFC7CE')
            elif acc < 70:
                add_cell_shading(acc_cell, 'FFEB9C')
            else:
                add_cell_shading(acc_cell, 'C6EFCE')
            acc_cell.paragraphs[0].runs[0].font.bold = True

        # Total row
        total_idx = len(metrics) + 1
        table.rows[total_idx].cells[0].text = 'TOTAL'
        table.rows[total_idx].cells[1].text = f"{self.content['total_planned']:.0f}"
        table.rows[total_idx].cells[2].text = f"{self.content['total_tbm_presence']:.0f}"
        table.rows[total_idx].cells[3].text = f"{self.content['total_verified']:.0f}"
        table.rows[total_idx].cells[4].text = f"{self.content['total_accuracy']:.0f}%"

        for cell in table.rows[total_idx].cells:
            cell.paragraphs[0].runs[0].font.bold = True

    # =========================================================================
    # CATEGORIES
    # =========================================================================

    def _add_category_header(self, number: int, title: str):
        """Add category header."""
        self.doc.add_paragraph()
        p = self.doc.add_paragraph()
        run = p.add_run(f'Category {number}: {title}')
        run.font.size = Pt(14)
        run.font.bold = True
        run.font.color.rgb = Colors.ACCENT_BLUE

    def _add_subheader(self, text: str):
        """Add subheader."""
        self.doc.add_paragraph()
        p = self.doc.add_paragraph()
        run = p.add_run(text)
        run.font.size = Pt(12)
        run.font.bold = True
        run.font.color.rgb = Colors.SECONDARY_BLUE

    def _add_alert_box(self, text: str, bg_hex: str = 'FADBD8', text_color=None):
        """Add alert box."""
        table = self.doc.add_table(rows=1, cols=1)
        cell = table.rows[0].cells[0]
        add_cell_shading(cell, bg_hex)
        add_cell_padding(cell, 150)

        p = cell.paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run(text)
        run.font.size = Pt(11)
        run.font.bold = True
        run.font.color.rgb = text_color or Colors.CRITICAL_RED

    def _add_observation_box(self, text: str):
        """Add shaded observation box."""
        table = self.doc.add_table(rows=1, cols=1)
        cell = table.rows[0].cells[0]
        add_cell_shading(cell, 'ECF0F1')
        add_cell_padding(cell, 100)

        p = cell.paragraphs[0]
        run = p.add_run(text)
        run.font.size = Pt(9)
        run.font.italic = True
        run.font.color.rgb = Colors.DARK_GRAY

    def _add_category_1(self):
        """Category 1: TBM Planning & Documentation Performance."""
        self._add_category_header(1, 'TBM Planning & Documentation Performance')

        narratives = self.content.get('narratives', {}).get('cat1', {})
        opening = narratives.get('opening', f"TBM verification shows {self.content['lpi']:.0f}% overall accuracy.")

        p = self.doc.add_paragraph()
        p.add_run(opening)

    def _add_category_2(self):
        """Category 2: Zero Verification Locations."""
        self._add_category_header(2, 'Zero Verification Locations')

        narratives = self.content.get('narratives', {}).get('cat2', {})
        intro = narratives.get('intro', 'The following locations had TBM commitments but zero field verification.')

        p = self.doc.add_paragraph()
        p.add_run(intro)

        self.doc.add_paragraph()

        # Alert box
        count = self.content['zero_verification_count']
        self._add_alert_box(f"{count} LOCATIONS WITH 0% VERIFICATION")

        # Group by contractor
        zero_locs = self.content['zero_verification_locations']
        contractors = set(z['company'] for z in zero_locs)

        for contractor in contractors:
            contractor_locs = [z for z in zero_locs if z['company'] == contractor]
            total_workers = sum(z['tbm_planned'] for z in contractor_locs)

            self._add_subheader(f"{contractor} - Zero Verification ({len(contractor_locs)} locations, {total_workers:.0f} workers)")

            # Table
            table = self.doc.add_table(rows=len(contractor_locs) + 1, cols=4)
            headers = ['Floor', 'Location', 'TBM', 'Verified']
            for i, h in enumerate(headers):
                table.rows[0].cells[i].text = h
                table.rows[0].cells[i].paragraphs[0].runs[0].font.bold = True

            for idx, loc in enumerate(contractor_locs, 1):
                table.rows[idx].cells[0].text = loc['floor']
                table.rows[idx].cells[1].text = loc['location']
                table.rows[idx].cells[2].text = f"{loc['tbm_planned']:.0f}"
                table.rows[idx].cells[3].text = '0'

    def _add_category_3(self):
        """Category 3: Idle Worker Observations - ACTUAL COST."""
        self._add_category_header(3, 'Idle Worker Observations - ACTUAL COST')

        narratives = self.content.get('narratives', {}).get('cat3', {})

        # Methodology
        methodology = narratives.get('methodology', f"CALCULATION METHODOLOGY: Idle time cost based on 1-hour idle duration per observation. Labor rate: ${self.content['labor_rate']}/hr.")
        self._add_alert_box(methodology, 'FDEBD0', Colors.WARNING_ORANGE)

        self.doc.add_paragraph()

        intro = narratives.get('intro', 'Field verification identified workers physically observed idle. This represents ACTUAL WASTE.')
        p = self.doc.add_paragraph()
        p.add_run(intro)

        self.doc.add_paragraph()

        # Alert with totals
        total_obs = len(self.content['idle_observations'])
        total_hours = self.content['idle_time_hours']
        total_cost = self.content['idle_time_cost']
        self._add_alert_box(f"IDLE TIME: {total_obs} OBSERVATIONS - {total_hours:.0f} WORKER-HOURS - ${total_cost:,.0f} DAILY WASTE")

        # Group by contractor
        idle_obs = self.content['idle_observations']
        contractors = set(o['company'] for o in idle_obs)

        for contractor in contractors:
            contractor_obs = [o for o in idle_obs if o['company'] == contractor]
            self._add_subheader(f"{contractor} - Idle Time Observations ({len(contractor_obs)} locations)")

            for obs in contractor_obs:
                self.doc.add_paragraph()

                # Task header
                p = self.doc.add_paragraph()
                header = f"{obs['task_id']}: {obs['location']}"
                if obs['level']:
                    header += f" ({obs['level']})"
                header += f" - {obs['status']}"
                run = p.add_run(header)
                run.font.size = Pt(11)
                run.font.bold = True
                run.font.color.rgb = Colors.CRITICAL_RED

                # Observation
                if obs['observation_text']:
                    self._add_observation_box(f"Field Observation ({obs['inspector']}): {obs['observation_text']}")

                # Analysis
                p = self.doc.add_paragraph()
                run = p.add_run('Analysis: ')
                run.font.bold = True
                p.add_run(obs['analysis'])

        # Cost summary table
        self._add_subheader('Idle Time Cost Summary')
        summary = self.content['idle_summary_by_contractor']

        table = self.doc.add_table(rows=len(summary) + 2, cols=4)
        headers = ['Contractor', 'Observations', 'Worker-Hours', f'Cost @ ${self.content["labor_rate"]}/hr']
        for i, h in enumerate(headers):
            table.rows[0].cells[i].text = h
            table.rows[0].cells[i].paragraphs[0].runs[0].font.bold = True

        for idx, s in enumerate(summary, 1):
            table.rows[idx].cells[0].text = s['contractor']
            table.rows[idx].cells[1].text = str(s['observations'])
            table.rows[idx].cells[2].text = f"{s['worker_hours']:.0f}"
            table.rows[idx].cells[3].text = f"${s['cost']:,.0f}"

        # Total
        total_idx = len(summary) + 1
        table.rows[total_idx].cells[0].text = 'TOTAL'
        table.rows[total_idx].cells[1].text = str(total_obs)
        table.rows[total_idx].cells[2].text = f"{total_hours:.0f}"
        table.rows[total_idx].cells[3].text = f"${total_cost:,.0f}"
        for cell in table.rows[total_idx].cells:
            cell.paragraphs[0].runs[0].font.bold = True

    def _add_category_4(self):
        """Category 4: High Verification Locations."""
        self._add_category_header(4, 'High Verification Locations - Best Practices')

        narratives = self.content.get('narratives', {}).get('cat4', {})
        intro = narratives.get('intro', 'Several locations showed excellent verification rates, demonstrating proper TBM implementation.')

        p = self.doc.add_paragraph()
        p.add_run(intro)

        self.doc.add_paragraph()

        # Table
        high_locs = self.content['high_verification_locations']
        if high_locs:
            table = self.doc.add_table(rows=len(high_locs) + 1, cols=5)
            headers = ['Contractor', 'Location', 'TBM', 'Verified', 'Accuracy']
            for i, h in enumerate(headers):
                table.rows[0].cells[i].text = h
                table.rows[0].cells[i].paragraphs[0].runs[0].font.bold = True

            for idx, loc in enumerate(high_locs, 1):
                table.rows[idx].cells[0].text = loc['company']
                table.rows[idx].cells[1].text = loc['location']
                table.rows[idx].cells[2].text = f"{loc['tbm_planned']:.0f}"
                table.rows[idx].cells[3].text = f"{loc['verified']:.0f}"
                table.rows[idx].cells[4].text = f"{loc['accuracy']:.0f}%"
                add_cell_shading(table.rows[idx].cells[4], 'C6EFCE')

        self.doc.add_paragraph()

        note = narratives.get('note', 'Note: Accuracy >100% indicates more workers verified than committed in TBM.')
        p = self.doc.add_paragraph()
        run = p.add_run(note)
        run.font.italic = True
        run.font.size = Pt(9)

    def _add_category_5(self):
        """Category 5: Key Findings."""
        self._add_category_header(5, 'Key Findings')

        for finding in self.content['key_findings']:
            p = self.doc.add_paragraph(finding, style='List Bullet')

    def _add_category_6(self):
        """Category 6: Contractor Performance Comparison."""
        self._add_category_header(6, 'Contractor Performance Comparison')

        for m in self.content['contractor_metrics']:
            self._add_subheader(f"{m['name']} Performance")

            table = self.doc.add_table(rows=7, cols=2)
            metrics = [
                ('Total Tasks', str(m['total_tasks'])),
                ('Total Planned', f"{m['total_planned']:.0f}"),
                ('Total Verified', f"{m['total_verified']:.0f}"),
                ('Accuracy', f"{m['accuracy']:.0f}%"),
                ('Idle Locations', str(m['idle_locations'])),
                ('Idle Cost', f"${m['idle_cost']:,.0f}"),
                ('Zero Verification', str(m['zero_verification_count'])),
            ]

            for idx, (label, value) in enumerate(metrics):
                table.rows[idx].cells[0].text = label
                table.rows[idx].cells[0].paragraphs[0].runs[0].font.bold = True
                table.rows[idx].cells[1].text = value

    def _add_category_7(self):
        """Category 7: TBM Process Limitations."""
        self._add_category_header(7, 'TBM Process Limitations')

        narratives = self.content.get('narratives', {}).get('cat7', {})

        self._add_subheader('7.1 Root Cause Analysis')
        root_cause = narratives.get('root_cause', f"The verification rates ({self.content['lpi']:.0f}%) may be affected by timing constraints.")
        p = self.doc.add_paragraph()
        p.add_run(root_cause)

        self._add_subheader('7.2 Impact on Report Accuracy')
        caveats = narratives.get('caveats', [
            'Unverified locations may reflect time constraints',
            'Idle observations limited to accessible areas',
        ])
        for caveat in caveats:
            self.doc.add_paragraph(caveat, style='List Bullet')

        self._add_subheader('7.3 Required Process Improvements')
        improvements = narratives.get('improvements', [
            'TBM submission deadline: 8:00 AM daily',
        ])
        for imp in improvements:
            self.doc.add_paragraph(imp, style='List Bullet')

    def _add_category_8(self):
        """Category 8: Conclusions & Recommendations."""
        self._add_category_header(8, 'Conclusions & Recommendations')

        narratives = self.content.get('narratives', {}).get('cat8', {})

        self._add_subheader('8.1 Primary Issues')
        issues = narratives.get('issues', ['TBM documentation gaps identified'])
        for issue in issues:
            self.doc.add_paragraph(issue, style='List Bullet')

        self._add_subheader('8.2 Recommendations')
        recommendations = narratives.get('recommendations', ['Improve TBM submission timing'])
        for rec in recommendations:
            p = self.doc.add_paragraph(style='List Bullet')
            if rec.startswith('CRITICAL:'):
                run = p.add_run(rec)
                run.font.bold = True
                run.font.color.rgb = Colors.CRITICAL_RED
            else:
                p.add_run(rec)

    def _add_end_marker(self):
        """Add end of report marker."""
        self.doc.add_paragraph()
        self.doc.add_paragraph()
        p = self.doc.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = p.add_run('— End of Report —')
        run.font.italic = True
        run.font.size = Pt(11)


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='Generate TBM Analysis DOCX')
    parser.add_argument('--input', required=True, help='Input JSON path')
    parser.add_argument('--output', help='Output DOCX path')

    args = parser.parse_args()

    # Load content
    input_path = Path(args.input)
    with open(input_path) as f:
        content = json.load(f)

    # Determine output path
    if args.output:
        output_path = args.output
    else:
        date_str = content.get('report_date', 'unknown')
        group = content.get('contractor_group', 'TBM')
        # Convert date format
        if len(date_str) == 8:
            parts = date_str.split('-')
            date_fmt = f"{parts[0]}.{parts[1]}.{parts[2]}"
        else:
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
                date_fmt = dt.strftime('%m.%d.%y')
            except:
                date_fmt = date_str

        output_path = input_path.parent / f"MXI - {group} TBM Analysis - {date_fmt}.docx"

    # Build report
    print(f"Generating: {output_path}")
    builder = TBMReportBuilder(content)
    builder.build(str(output_path))
    print(f"Done: {output_path}")


if __name__ == '__main__':
    main()
