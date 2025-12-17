---
name: mxi-powerpoint
description: Create MXI-branded PowerPoint presentations for Samsung Taylor FAB1 project. Use when creating slides, presentations, executive summaries, analysis reports, or progress updates. Generates both content and .pptx files using python-pptx.
---

# MXI PowerPoint Presentations

Create professional PowerPoint presentations for the Samsung Taylor FAB1 delay analysis project.

## Quick Start (Recommended)

Use the `MXIPresentation` helper class which automatically includes logos and branding:

```python
import sys
sys.path.insert(0, '/home/pcuriel/samsung-project/.claude/skills/mxi-powerpoint/scripts')
from pptx_helpers import MXIPresentation

# Create presentation with automatic MXI branding and logos
prs = MXIPresentation(
    "Samsung Taylor FAB1 Analysis",
    "Schedule Delay Attribution Report",
    show_logo=True,           # MXI logo on all slides (default: True)
    show_samsung_logo=False   # Samsung logo co-branding (default: False)
)

# Add slides
prs.add_content_slide("Key Findings", [
    "Schedule slipped 127 days from baseline",
    "Scope grew by 2,847 tasks (34% increase)",
])

prs.add_kpi_slide("Performance Metrics", [
    {"label": "Schedule Slip", "value": "127 days", "trend": "down"},
    {"label": "Scope Growth", "value": "+34%", "trend": "down"},
])

# Save to OneDrive presentations folder
prs.save()
```

## MXI Brand Assets

Logo files are automatically loaded from OneDrive:

| Asset | Path | Usage |
|-------|------|-------|
| MXI Logo (Full) | `UI/MXI Logo Full.png` | Title slides (bottom-right) |
| MXI Logo (Transparent) | `UI/MXI Logo - Transparent.png` | Content slide headers (top-left) |
| Samsung Logo | `UI/Samsung Logo.png` | Co-branding (top-right, optional) |
| Light Background | `UI/MXI Light Background.png` | Optional slide backgrounds |

**Asset Location:** `/mnt/c/Users/pcuri/OneDrive - MXI/Desktop/Samsung Dashboard/UI/`

## MXI Brand Guidelines

### Colors
- **MXI Navy (Primary):** RGB(0, 51, 102) / #003366
- **MXI Blue (Accent):** RGB(0, 112, 192) / #0070C0
- **MXI Gray (Text):** RGB(89, 89, 89) / #595959
- **Samsung Blue:** RGB(20, 40, 160) / #1428A0
- **Alert Red:** RGB(192, 0, 0) / #C00000
- **Success Green:** RGB(0, 128, 0) / #008000

### Fonts
- **Titles:** Segoe UI Bold, 28-36pt
- **Subtitles:** Segoe UI, 18-24pt
- **Body Text:** Segoe UI, 14-18pt
- **Footer/Page Numbers:** Segoe UI Semibold, 12pt
- **Footnotes:** Segoe UI, 10-12pt

### Layout Standards
- Left margin: 0.5 inches
- Right margin: 0.5 inches
- Top margin: 0.75 inches (after title)
- Footer height: 0.5 inches

## Presentation Types

### 1. Executive Summary
For Samsung leadership - high-level findings and recommendations.

**Structure:**
1. Title slide with date and "Prepared by MXI"
2. Executive summary (3-5 key findings)
3. Key metrics dashboard
4. Timeline/milestones
5. Recommendations
6. Next steps

### 2. Analysis Report
Detailed technical analysis with supporting data.

**Structure:**
1. Title slide
2. Analysis objectives
3. Methodology
4. Data sources
5. Findings (multiple slides)
6. Charts and visualizations
7. Conclusions
8. Appendix (data tables)

### 3. Progress Update
Weekly/monthly status updates.

**Structure:**
1. Title slide with reporting period
2. Period highlights (3-5 bullets)
3. KPI dashboard
4. Work completed
5. Issues/blockers
6. Next period plan

## Slide Templates

### Title Slide

```python
def create_title_slide(prs, title, subtitle, date):
    slide_layout = prs.slide_layouts[6]  # Blank
    slide = prs.slides.add_slide(slide_layout)

    # Navy background bar at top
    shape = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, 0, 0, prs.slide_width, Inches(2.5)
    )
    shape.fill.solid()
    shape.fill.fore_color.rgb = RGBColor(0, 51, 102)
    shape.line.fill.background()

    # Title
    title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.75), Inches(12), Inches(1))
    tf = title_box.text_frame
    p = tf.paragraphs[0]
    p.text = title
    p.font.size = Pt(36)
    p.font.bold = True
    p.font.color.rgb = RGBColor(255, 255, 255)

    # Subtitle
    sub_box = slide.shapes.add_textbox(Inches(0.5), Inches(1.75), Inches(12), Inches(0.5))
    tf = sub_box.text_frame
    p = tf.paragraphs[0]
    p.text = subtitle
    p.font.size = Pt(20)
    p.font.color.rgb = RGBColor(200, 200, 200)

    # Date and branding
    footer_box = slide.shapes.add_textbox(Inches(0.5), Inches(6.5), Inches(12), Inches(0.5))
    tf = footer_box.text_frame
    p = tf.paragraphs[0]
    p.text = f"{date} | Prepared by MXI"
    p.font.size = Pt(14)
    p.font.color.rgb = RGBColor(89, 89, 89)

    return slide
```

### Content Slide with Bullets

```python
def create_content_slide(prs, title, bullets):
    slide_layout = prs.slide_layouts[6]  # Blank
    slide = prs.slides.add_slide(slide_layout)

    # Title bar
    title_shape = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE, 0, 0, prs.slide_width, Inches(1.2)
    )
    title_shape.fill.solid()
    title_shape.fill.fore_color.rgb = RGBColor(0, 51, 102)
    title_shape.line.fill.background()

    # Title text
    title_box = slide.shapes.add_textbox(Inches(0.5), Inches(0.35), Inches(12), Inches(0.7))
    tf = title_box.text_frame
    p = tf.paragraphs[0]
    p.text = title
    p.font.size = Pt(28)
    p.font.bold = True
    p.font.color.rgb = RGBColor(255, 255, 255)

    # Bullet content
    content_box = slide.shapes.add_textbox(Inches(0.5), Inches(1.5), Inches(12), Inches(5))
    tf = content_box.text_frame
    tf.word_wrap = True

    for i, bullet in enumerate(bullets):
        if i == 0:
            p = tf.paragraphs[0]
        else:
            p = tf.add_paragraph()
        p.text = f"• {bullet}"
        p.font.size = Pt(18)
        p.font.color.rgb = RGBColor(89, 89, 89)
        p.space_after = Pt(12)

    return slide
```

### Two-Column Slide

```python
def create_two_column_slide(prs, title, left_content, right_content):
    slide = prs.slides.add_slide(prs.slide_layouts[6])

    # Title bar (same as content slide)
    # ... add title bar code ...

    # Left column
    left_box = slide.shapes.add_textbox(Inches(0.5), Inches(1.5), Inches(5.8), Inches(5))
    # ... add content ...

    # Right column
    right_box = slide.shapes.add_textbox(Inches(6.8), Inches(1.5), Inches(5.8), Inches(5))
    # ... add content ...

    return slide
```

## Adding Charts

Use matplotlib to create charts, save as images, and insert:

```python
import matplotlib.pyplot as plt
from io import BytesIO

def add_chart_to_slide(slide, fig, left, top, width, height):
    image_stream = BytesIO()
    fig.savefig(image_stream, format='png', dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    image_stream.seek(0)
    slide.shapes.add_picture(image_stream, left, top, width, height)
    plt.close(fig)
```

## Adding Tables

```python
from pptx.util import Inches, Pt

def add_table(slide, data, left, top, width, height):
    rows, cols = len(data), len(data[0])
    table = slide.shapes.add_table(rows, cols, left, top, width, height).table

    # Header row styling
    for j, cell in enumerate(table.rows[0].cells):
        cell.text = str(data[0][j])
        cell.fill.solid()
        cell.fill.fore_color.rgb = RGBColor(0, 51, 102)
        p = cell.text_frame.paragraphs[0]
        p.font.color.rgb = RGBColor(255, 255, 255)
        p.font.bold = True
        p.font.size = Pt(12)

    # Data rows
    for i in range(1, rows):
        for j, cell in enumerate(table.rows[i].cells):
            cell.text = str(data[i][j])
            p = cell.text_frame.paragraphs[0]
            p.font.size = Pt(11)

    return table
```

## Footer Standard

Footer appears on a banner at the bottom of every slide (except title). Uses Segoe UI Semibold font.

**Format:** `[Page #]  MXI  |  Samsung Taylor FAB1  |  [Report Title]  |  [Date]`

The footer is automatically added by `MXIPresentation` and includes:
- Page number (left side)
- Company branding (MXI)
- Project name (Samsung Taylor FAB1)
- Report title (from presentation title)
- Date (month/year)

## Complete Example

```python
import sys
sys.path.insert(0, '/home/pcuriel/samsung-project/.claude/skills/mxi-powerpoint/scripts')
from pptx_helpers import MXIPresentation

# Create presentation with MXI branding and logos
prs = MXIPresentation(
    "Samsung Taylor FAB1 Analysis",
    "Q4 2025 Progress Report",
    show_logo=True,
    show_samsung_logo=True  # Include Samsung logo for co-branding
)

# Executive summary
prs.add_content_slide("Executive Summary", [
    "Schedule analysis complete for 66 YATES snapshots",
    "Identified 127-day slip from baseline schedule",
    "Scope grew by 2,847 tasks (34% increase)",
    "1,108 issues documented and categorized"
])

# Section divider
prs.add_section_slide("Key Findings", 1)

# KPI dashboard
prs.add_kpi_slide("Performance Metrics", [
    {"label": "Schedule Slip", "value": "127 days", "trend": "down"},
    {"label": "Scope Growth", "value": "+34%", "trend": "down"},
    {"label": "Issues Found", "value": "1,108", "trend": "flat"},
    {"label": "Data Sources", "value": "66", "trend": "up"}
])

# Data table
prs.add_table_slide("Delay Attribution", [
    ["Category", "Days", "% of Total", "Primary Cause"],
    ["Coordination", "45", "35%", "Trade conflicts"],
    ["Rework", "32", "25%", "Quality issues"],
    ["Design Changes", "28", "22%", "Client requests"],
    ["Weather", "12", "9%", "Rain delays"],
])

# Two-column slide
prs.add_two_column_slide(
    "Next Steps",
    "Completed", [
        "Schedule variance analysis",
        "Issue categorization",
        "Labor correlation study",
    ],
    "In Progress", [
        "Quality impact assessment",
        "Cost reconciliation",
        "Final report preparation",
    ]
)

# Save to OneDrive
prs.save()  # Auto-generates filename: 20251216_samsung_taylor_fab1_analysis.pptx
```

## Requirements

```bash
pip install python-pptx matplotlib pandas
```

## File Naming Convention

- `YYYYMMDD_[type]_[topic].pptx`
- Examples:
  - `20251216_exec_summary_q4_findings.pptx`
  - `20251216_analysis_scope_growth.pptx`
  - `20251216_progress_weekly_update.pptx`

## Output Location

Save presentations to:
```
/mnt/c/Users/pcuri/OneDrive - MXI/Desktop/Samsung Dashboard/Presentations/
```

For detailed helper functions, see [scripts/pptx_helpers.py](scripts/pptx_helpers.py).
