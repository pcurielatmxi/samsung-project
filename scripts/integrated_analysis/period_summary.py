"""
Period Summary Query Script
===========================

Extracts and summarizes data for a given period (year-month) from multiple sources:
- Labor hours by trade/company (ProjectSight, TBM)
- Schedule progress and delays (P6 slippage analysis)
- Quality issues by trade (RABA, PSI)
- Narrative statements (delay claims, quality issues, coordination)

Designed for LLM consumption with context window controls.

Usage:
    python -m scripts.integrated_analysis.period_summary 2024-06
    python -m scripts.integrated_analysis.period_summary 2024-06 --detail
    python -m scripts.integrated_analysis.period_summary 2024-06 --section labor
    python -m scripts.integrated_analysis.period_summary 2024-06 --top-n 10 --format json
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json
import sys
import argparse

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config.settings import settings


###############################################################################
# CONFIGURATION
###############################################################################

# Default limits for context control
DEFAULT_TOP_N = 15
MAX_TOP_N = 50

# Token estimates (rough)
TOKENS_PER_ROW_SUMMARY = 50
TOKENS_PER_ROW_DETAIL = 150


###############################################################################
# DATA LOADERS
###############################################################################

class PeriodDataLoader:
    """Load and cache data for a specific period."""

    def __init__(self, year: int, month: int):
        self.year = year
        self.month = month
        self.period_str = f"{year}-{month:02d}"
        self._cache = {}

    def _get_period_filter(self, df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        """Filter dataframe to the specified period."""
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        mask = (df[date_col].dt.year == self.year) & (df[date_col].dt.month == self.month)
        return df[mask]

    def load_labor_projectsight(self) -> pd.DataFrame:
        """Load ProjectSight labor entries for the period."""
        if 'labor_ps' in self._cache:
            return self._cache['labor_ps']

        path = settings.PROJECTSIGHT_PROCESSED_DIR / 'labor_entries_enriched.csv'
        if not path.exists():
            return pd.DataFrame()

        df = pd.read_csv(path, low_memory=False)
        df = self._get_period_filter(df, 'report_date')
        self._cache['labor_ps'] = df
        return df

    def load_labor_tbm(self) -> pd.DataFrame:
        """Load TBM work entries for the period."""
        if 'labor_tbm' in self._cache:
            return self._cache['labor_tbm']

        path = settings.TBM_PROCESSED_DIR / 'work_entries_enriched.csv'
        if not path.exists():
            return pd.DataFrame()

        df = pd.read_csv(path, low_memory=False)
        df = self._get_period_filter(df, 'report_date')
        self._cache['labor_tbm'] = df
        return df

    def load_quality_raba(self) -> pd.DataFrame:
        """Load RABA quality inspections for the period."""
        if 'quality_raba' in self._cache:
            return self._cache['quality_raba']

        path = settings.RABA_PROCESSED_DIR / 'raba_consolidated.csv'
        if not path.exists():
            return pd.DataFrame()

        df = pd.read_csv(path, low_memory=False)
        df = self._get_period_filter(df, 'report_date_normalized')
        self._cache['quality_raba'] = df
        return df

    def load_quality_psi(self) -> pd.DataFrame:
        """Load PSI quality inspections for the period."""
        if 'quality_psi' in self._cache:
            return self._cache['quality_psi']

        path = settings.PSI_PROCESSED_DIR / 'psi_consolidated.csv'
        if not path.exists():
            return pd.DataFrame()

        df = pd.read_csv(path, low_memory=False)
        df = self._get_period_filter(df, 'report_date_normalized')
        self._cache['quality_psi'] = df
        return df

    def load_schedule_slippage(self) -> dict:
        """Load schedule slippage analysis for the period."""
        if 'slippage' in self._cache:
            return self._cache['slippage']

        try:
            from scripts.integrated_analysis.schedule_slippage_analysis import ScheduleSlippageAnalyzer
            analyzer = ScheduleSlippageAnalyzer()
            result = analyzer.analyze_month(self.year, self.month)
            self._cache['slippage'] = result
            return result
        except Exception as e:
            print(f"Warning: Could not load schedule slippage: {e}", file=sys.stderr)
            return None

    def load_narratives(self) -> list:
        """Load narrative statements for the period."""
        if 'narratives' in self._cache:
            return self._cache['narratives']

        # Load from stage 4 (refine) output
        refine_dir = settings.NARRATIVES_PROCESSED_DIR / '4.refine'
        if not refine_dir.exists():
            return []

        statements = []
        period_start = datetime(self.year, self.month, 1)
        if self.month == 12:
            period_end = datetime(self.year + 1, 1, 1)
        else:
            period_end = datetime(self.year, self.month + 1, 1)

        for f in refine_dir.glob('*.json'):
            if '.error' in f.name:
                continue
            try:
                data = json.loads(f.read_text())
                content = data.get('content', {})
                file_statements = content.get('statements', [])

                for stmt in file_statements:
                    # Parse event_date if available
                    event_date_str = stmt.get('event_date', '')
                    event_date = None
                    if event_date_str:
                        try:
                            event_date = datetime.strptime(event_date_str, '%Y-%m-%d')
                        except:
                            pass

                    # Include if event_date in period, or if no date (include by document)
                    include = False
                    if event_date and period_start <= event_date < period_end:
                        include = True
                    elif not event_date:
                        # Check document metadata date
                        doc_date_str = data.get('metadata', {}).get('document_date', '')
                        if doc_date_str:
                            try:
                                doc_date = datetime.strptime(doc_date_str[:10], '%Y-%m-%d')
                                if period_start <= doc_date < period_end:
                                    include = True
                            except:
                                pass

                    if include:
                        statements.append({
                            'text': stmt.get('text', ''),
                            'category': stmt.get('category', 'other'),
                            'event_date': event_date_str,
                            'parties': stmt.get('parties', []),
                            'locations': stmt.get('locations', []),
                            'impact_days': stmt.get('impact_days'),
                            'source_file': f.stem.replace('.refine', ''),
                        })
            except Exception as e:
                continue

        self._cache['narratives'] = statements
        return statements


###############################################################################
# SUMMARY GENERATORS
###############################################################################

class LaborSummary:
    """Generate labor hour summaries."""

    @staticmethod
    def by_trade(loader: PeriodDataLoader, top_n: int = DEFAULT_TOP_N) -> dict:
        """Summarize labor hours by trade."""
        df = loader.load_labor_projectsight()

        if len(df) == 0:
            return {'status': 'no_data', 'period': loader.period_str}

        # Aggregate by trade
        by_trade = df.groupby('trade_name').agg(
            total_hours=('hours_new', 'sum'),
            headcount=('person_name', 'nunique'),
            companies=('company', 'nunique'),
            days_worked=('report_date', 'nunique')
        ).reset_index()

        by_trade = by_trade.sort_values('total_hours', ascending=False)
        by_trade['pct_of_total'] = (by_trade['total_hours'] / by_trade['total_hours'].sum() * 100).round(1)

        return {
            'status': 'ok',
            'period': loader.period_str,
            'total_hours': int(by_trade['total_hours'].sum()),
            'total_headcount': int(df['person_name'].nunique()),
            'total_companies': int(df['company'].nunique()),
            'by_trade': by_trade.head(top_n).to_dict('records'),
            'trades_shown': min(top_n, len(by_trade)),
            'trades_total': len(by_trade),
        }

    @staticmethod
    def by_company(loader: PeriodDataLoader, top_n: int = DEFAULT_TOP_N) -> dict:
        """Summarize labor hours by company."""
        df = loader.load_labor_projectsight()

        if len(df) == 0:
            return {'status': 'no_data', 'period': loader.period_str}

        by_company = df.groupby('company').agg(
            total_hours=('hours_new', 'sum'),
            headcount=('person_name', 'nunique'),
            trades=('trade_name', 'nunique'),
            days_worked=('report_date', 'nunique')
        ).reset_index()

        by_company = by_company.sort_values('total_hours', ascending=False)
        by_company['pct_of_total'] = (by_company['total_hours'] / by_company['total_hours'].sum() * 100).round(1)

        return {
            'status': 'ok',
            'period': loader.period_str,
            'total_hours': int(by_company['total_hours'].sum()),
            'by_company': by_company.head(top_n).to_dict('records'),
            'companies_shown': min(top_n, len(by_company)),
            'companies_total': len(by_company),
        }


class ScheduleSummary:
    """Generate schedule progress and delay summaries."""

    @staticmethod
    def slippage_overview(loader: PeriodDataLoader) -> dict:
        """Get high-level schedule slippage overview."""
        result = loader.load_schedule_slippage()

        if result is None:
            return {'status': 'no_data', 'period': loader.period_str}

        metrics = result.get('project_metrics', {})
        tasks = result.get('tasks')

        return {
            'status': 'ok',
            'period': loader.period_str,
            'project_finish_prev': str(metrics.get('project_finish_prev', '')),
            'project_finish_curr': str(metrics.get('project_finish_curr', '')),
            'slippage_days': metrics.get('project_slippage_days', 0),
            'driving_path_tasks': metrics.get('driving_path_tasks_curr', 0),
            'tasks_compared': len(tasks) if tasks is not None else 0,
        }

    @staticmethod
    def delay_drivers(loader: PeriodDataLoader, top_n: int = DEFAULT_TOP_N) -> dict:
        """Get top delay-causing tasks."""
        result = loader.load_schedule_slippage()

        if result is None:
            return {'status': 'no_data', 'period': loader.period_str}

        tasks = result.get('tasks')
        if tasks is None or len(tasks) == 0:
            return {'status': 'no_tasks', 'period': loader.period_str}

        # Filter to tasks with positive own_delay
        delayers = tasks[tasks['own_delay_days'] > 0].copy()
        delayers = delayers.sort_values('own_delay_days', ascending=False)

        # Separate driving path vs non-driving
        driving = delayers[delayers['on_driving_path'] == True].head(top_n)
        other = delayers[delayers['on_driving_path'] == False].head(top_n)

        def task_to_dict(row):
            return {
                'task_code': row['task_code'],
                'task_name': row['task_name'][:60] if pd.notna(row['task_name']) else '',
                'own_delay_days': round(row['own_delay_days'], 1),
                'inherited_delay_days': round(row['inherited_delay_days'], 1),
                'is_critical': bool(row['is_critical']),
                'status': row['status'],
            }

        return {
            'status': 'ok',
            'period': loader.period_str,
            'total_delaying_tasks': len(delayers),
            'driving_path_delayers': [task_to_dict(row) for _, row in driving.iterrows()],
            'other_delayers': [task_to_dict(row) for _, row in other.iterrows()],
        }

    @staticmethod
    def delay_by_category(loader: PeriodDataLoader) -> dict:
        """Summarize delays by category."""
        result = loader.load_schedule_slippage()

        if result is None:
            return {'status': 'no_data', 'period': loader.period_str}

        tasks = result.get('tasks')
        if tasks is None or len(tasks) == 0:
            return {'status': 'no_tasks', 'period': loader.period_str}

        by_category = tasks.groupby('delay_category').agg(
            task_count=('task_code', 'count'),
            avg_own_delay=('own_delay_days', 'mean'),
            total_own_delay=('own_delay_days', 'sum'),
        ).reset_index()

        by_category = by_category.sort_values('task_count', ascending=False)

        return {
            'status': 'ok',
            'period': loader.period_str,
            'by_category': by_category.to_dict('records'),
        }


class QualitySummary:
    """Generate quality inspection summaries."""

    @staticmethod
    def overview(loader: PeriodDataLoader) -> dict:
        """Get high-level quality overview combining RABA and PSI."""
        raba = loader.load_quality_raba()
        psi = loader.load_quality_psi()

        def get_stats(df, source):
            if len(df) == 0:
                return None

            # Check for outcome column (RABA/PSI use 'outcome')
            if 'outcome' in df.columns:
                # outcome values: pass, fail, partial, etc.
                passed = df['outcome'].str.lower().str.contains('pass|accept|approved', na=False).sum()
                failed = df['outcome'].str.lower().str.contains('fail|reject', na=False).sum()
                pass_rate = round(passed / len(df) * 100, 1) if len(df) > 0 else 0
                fail_rate = round(failed / len(df) * 100, 1) if len(df) > 0 else 0
            else:
                pass_rate = None
                fail_rate = None

            return {
                'source': source,
                'total_inspections': len(df),
                'pass_rate_pct': pass_rate,
                'fail_rate_pct': fail_rate,
                'unique_dates': df['report_date_normalized'].nunique() if 'report_date_normalized' in df.columns else 0,
            }

        raba_stats = get_stats(raba, 'RABA')
        psi_stats = get_stats(psi, 'PSI')

        return {
            'status': 'ok',
            'period': loader.period_str,
            'raba': raba_stats,
            'psi': psi_stats,
            'total_inspections': (len(raba) + len(psi)),
        }

    @staticmethod
    def by_trade(loader: PeriodDataLoader, top_n: int = DEFAULT_TOP_N) -> dict:
        """Summarize quality issues by trade."""
        raba = loader.load_quality_raba()
        psi = loader.load_quality_psi()

        results = []

        for df, source in [(raba, 'RABA'), (psi, 'PSI')]:
            if len(df) == 0:
                continue

            # Find trade column - RABA/PSI use dim_trade_code, PSI also has 'trade'
            trade_col = None
            for col in ['dim_trade_code', 'trade', 'trade_name']:
                if col in df.columns:
                    trade_col = col
                    break

            if trade_col is None:
                continue

            # Calculate pass/fail by trade
            df_copy = df.copy()
            if 'outcome' in df_copy.columns:
                df_copy['passed'] = df_copy['outcome'].str.lower().str.contains('pass|accept', na=False)
                df_copy['failed'] = df_copy['outcome'].str.lower().str.contains('fail|reject', na=False)
            else:
                df_copy['passed'] = False
                df_copy['failed'] = False

            by_trade = df_copy.groupby(trade_col).agg(
                inspections=('inspection_id', 'count'),
                passed=('passed', 'sum'),
                failed=('failed', 'sum'),
            ).reset_index()
            by_trade.columns = ['trade', 'inspections', 'passed', 'failed']
            by_trade['pass_rate'] = (by_trade['passed'] / by_trade['inspections'] * 100).round(1)

            by_trade = by_trade.sort_values('inspections', ascending=False)
            by_trade['source'] = source
            results.append(by_trade.head(top_n))

        if not results:
            return {'status': 'no_data', 'period': loader.period_str}

        combined = pd.concat(results, ignore_index=True)

        return {
            'status': 'ok',
            'period': loader.period_str,
            'by_trade': combined.to_dict('records'),
        }

    @staticmethod
    def by_location(loader: PeriodDataLoader, top_n: int = DEFAULT_TOP_N) -> dict:
        """Summarize quality issues by location (building/level)."""
        raba = loader.load_quality_raba()
        psi = loader.load_quality_psi()

        results = []

        for df, source in [(raba, 'RABA'), (psi, 'PSI')]:
            if len(df) == 0 or 'building' not in df.columns:
                continue

            df = df.copy()
            df['location'] = df['building'].fillna('') + '-' + df['level'].fillna('').astype(str)

            by_loc = df.groupby('location').agg(
                inspections=('inspection_id', 'count'),
            ).reset_index()

            by_loc = by_loc.sort_values('inspections', ascending=False)
            by_loc['source'] = source
            results.append(by_loc.head(top_n))

        if not results:
            return {'status': 'no_data', 'period': loader.period_str}

        combined = pd.concat(results, ignore_index=True)

        return {
            'status': 'ok',
            'period': loader.period_str,
            'by_location': combined.to_dict('records'),
        }

    @staticmethod
    def failures(loader: PeriodDataLoader, top_n: int = DEFAULT_TOP_N) -> dict:
        """Get details of failed inspections."""
        raba = loader.load_quality_raba()
        psi = loader.load_quality_psi()

        failures = []

        for df, source in [(raba, 'RABA'), (psi, 'PSI')]:
            if len(df) == 0:
                continue

            # Use 'outcome' column (RABA/PSI standard)
            if 'outcome' not in df.columns:
                continue

            failed = df[df['outcome'].str.lower().str.contains('fail|reject', na=False)].copy()
            failed['source'] = source
            failures.append(failed)

        if not failures:
            return {'status': 'no_failures', 'period': loader.period_str}

        combined = pd.concat(failures, ignore_index=True)
        combined = combined.sort_values('report_date_normalized', ascending=False)

        def failure_to_dict(row):
            # Get trade from available columns
            trade = row.get('dim_trade_code', row.get('trade', ''))
            # Get company from available columns
            company = row.get('contractor', row.get('subcontractor', ''))
            # Get failure reason
            failure_reason = row.get('failure_reason', row.get('failure_category', ''))

            return {
                'source': row.get('source', ''),
                'inspection_id': row.get('inspection_id', ''),
                'date': str(row.get('report_date_normalized', ''))[:10],
                'building': row.get('building', ''),
                'level': row.get('level', ''),
                'trade': trade,
                'company': company,
                'failure_reason': failure_reason,
            }

        return {
            'status': 'ok',
            'period': loader.period_str,
            'total_failures': len(combined),
            'failures': [failure_to_dict(row) for _, row in combined.head(top_n).iterrows()],
        }


class NarrativeSummary:
    """Generate narrative statement summaries."""

    # Statement categories with descriptions
    CATEGORIES = {
        'delay': 'Delay claims and schedule impacts',
        'quality_issue': 'Quality problems and defects',
        'coordination': 'Coordination issues between parties',
        'scope_change': 'Scope changes and modifications',
        'owner_direction': 'Owner directives and decisions',
        'design_issue': 'Design problems or RFIs',
        'resource': 'Resource and manpower issues',
        'dispute': 'Disputes and disagreements',
        'weather': 'Weather-related impacts',
        'safety': 'Safety incidents or concerns',
        'progress': 'Progress updates and status',
        'other': 'Other statements',
    }

    @staticmethod
    def overview(loader: PeriodDataLoader) -> dict:
        """Get high-level narrative overview."""
        statements = loader.load_narratives()

        if not statements:
            return {'status': 'no_data', 'period': loader.period_str}

        # Count by category
        by_category = {}
        for stmt in statements:
            cat = stmt.get('category', 'other')
            by_category[cat] = by_category.get(cat, 0) + 1

        # Count statements with impact days
        with_impact = [s for s in statements if s.get('impact_days')]
        total_impact_days = sum(s.get('impact_days', 0) or 0 for s in with_impact)

        # Unique parties mentioned
        all_parties = set()
        for stmt in statements:
            parties = stmt.get('parties') or []
            all_parties.update(parties)

        return {
            'status': 'ok',
            'period': loader.period_str,
            'total_statements': len(statements),
            'by_category': by_category,
            'statements_with_impact': len(with_impact),
            'total_impact_days_claimed': total_impact_days,
            'unique_parties': len(all_parties),
            'parties': list(all_parties)[:10],
        }

    @staticmethod
    def by_category(loader: PeriodDataLoader, categories: list = None, top_n: int = DEFAULT_TOP_N) -> dict:
        """Get statements grouped by category."""
        statements = loader.load_narratives()

        if not statements:
            return {'status': 'no_data', 'period': loader.period_str}

        # Filter to requested categories
        if categories:
            statements = [s for s in statements if s.get('category') in categories]

        # Group by category
        grouped = {}
        for stmt in statements:
            cat = stmt.get('category', 'other')
            if cat not in grouped:
                grouped[cat] = []
            grouped[cat].append(stmt)

        # Format output - limit per category
        result = {}
        for cat, stmts in grouped.items():
            # Sort by impact_days (descending) then by text length
            stmts.sort(key=lambda s: (-(s.get('impact_days') or 0), -len(s.get('text', ''))))
            result[cat] = {
                'count': len(stmts),
                'statements': [
                    {
                        'text': s['text'][:200] + ('...' if len(s['text']) > 200 else ''),
                        'parties': s.get('parties', []),
                        'impact_days': s.get('impact_days'),
                        'source': s.get('source_file', '')[:40],
                    }
                    for s in stmts[:top_n]
                ]
            }

        return {
            'status': 'ok',
            'period': loader.period_str,
            'categories': result,
        }

    @staticmethod
    def delay_claims(loader: PeriodDataLoader, top_n: int = DEFAULT_TOP_N) -> dict:
        """Get delay-related statements with impact days."""
        statements = loader.load_narratives()

        if not statements:
            return {'status': 'no_data', 'period': loader.period_str}

        # Filter to delay-related categories
        delay_categories = ['delay', 'coordination', 'quality_issue', 'design_issue', 'resource']
        delay_stmts = [s for s in statements if s.get('category') in delay_categories]

        # Sort by impact_days
        delay_stmts.sort(key=lambda s: -(s.get('impact_days') or 0))

        return {
            'status': 'ok',
            'period': loader.period_str,
            'total_delay_statements': len(delay_stmts),
            'statements': [
                {
                    'category': s.get('category'),
                    'text': s['text'][:300] + ('...' if len(s['text']) > 300 else ''),
                    'parties': s.get('parties', []),
                    'locations': s.get('locations', []),
                    'impact_days': s.get('impact_days'),
                    'source': s.get('source_file', '')[:40],
                }
                for s in delay_stmts[:top_n]
            ],
        }


###############################################################################
# HELPER UTILITIES FOR LLM DRILL-DOWN
###############################################################################

class DetailQueries:
    """Helper utilities for LLM to get more context."""

    @staticmethod
    def get_task_details(task_code: str, year: int, month: int) -> dict:
        """Get detailed information about a specific task."""
        try:
            from scripts.integrated_analysis.schedule_slippage_analysis import ScheduleSlippageAnalyzer
            analyzer = ScheduleSlippageAnalyzer()
            result = analyzer.analyze_month(year, month)

            if result is None:
                return {'status': 'no_data'}

            tasks = result.get('tasks')
            if tasks is None:
                return {'status': 'no_tasks'}

            task = tasks[tasks['task_code'] == task_code]
            if len(task) == 0:
                return {'status': 'task_not_found', 'task_code': task_code}

            row = task.iloc[0]
            return {
                'status': 'ok',
                'task_code': task_code,
                'task_name': row.get('task_name', ''),
                'status': row.get('status', ''),
                'own_delay_days': row.get('own_delay_days', 0),
                'inherited_delay_days': row.get('inherited_delay_days', 0),
                'finish_slip_days': row.get('finish_slip_days', 0),
                'start_slip_days': row.get('start_slip_days', 0),
                'float_curr_days': row.get('float_curr_days', 0),
                'float_change_days': row.get('float_change_days', 0),
                'is_critical': bool(row.get('is_critical', False)),
                'on_driving_path': bool(row.get('on_driving_path', False)),
                'delay_category': row.get('delay_category', ''),
                'early_start_curr': str(row.get('early_start_curr', '')),
                'early_end_curr': str(row.get('early_end_curr', '')),
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}

    @staticmethod
    def get_company_labor(company: str, year: int, month: int) -> dict:
        """Get labor details for a specific company in a period."""
        loader = PeriodDataLoader(year, month)
        df = loader.load_labor_projectsight()

        if len(df) == 0:
            return {'status': 'no_data'}

        company_df = df[df['company'].str.lower() == company.lower()]
        if len(company_df) == 0:
            # Try partial match
            company_df = df[df['company'].str.lower().str.contains(company.lower(), na=False)]

        if len(company_df) == 0:
            return {'status': 'company_not_found', 'company': company}

        by_trade = company_df.groupby('trade_name').agg(
            hours=('hours_new', 'sum'),
            headcount=('person_name', 'nunique'),
        ).reset_index().sort_values('hours', ascending=False)

        return {
            'status': 'ok',
            'company': company,
            'period': loader.period_str,
            'total_hours': int(company_df['hours_new'].sum()),
            'headcount': int(company_df['person_name'].nunique()),
            'days_worked': int(company_df['report_date'].nunique()),
            'by_trade': by_trade.to_dict('records'),
        }

    @staticmethod
    def get_inspection_details(inspection_id: str) -> dict:
        """Get details of a specific inspection."""
        # Try RABA first
        raba_path = settings.RABA_PROCESSED_DIR / 'raba_consolidated.csv'
        if raba_path.exists():
            df = pd.read_csv(raba_path, low_memory=False)
            match = df[df['inspection_id'] == inspection_id]
            if len(match) > 0:
                row = match.iloc[0]
                return {
                    'status': 'ok',
                    'source': 'RABA',
                    'inspection_id': inspection_id,
                    'date': str(row.get('report_date_normalized', ''))[:10],
                    'building': row.get('building', ''),
                    'level': row.get('level', ''),
                    'trade': row.get('trade_name', ''),
                    'company': row.get('company_name', ''),
                    'result': row.get('result', ''),
                    'findings': row.get('findings', ''),
                }

        # Try PSI
        psi_path = settings.PSI_PROCESSED_DIR / 'psi_consolidated.csv'
        if psi_path.exists():
            df = pd.read_csv(psi_path, low_memory=False)
            match = df[df['inspection_id'] == inspection_id]
            if len(match) > 0:
                row = match.iloc[0]
                return {
                    'status': 'ok',
                    'source': 'PSI',
                    'inspection_id': inspection_id,
                    'date': str(row.get('report_date_normalized', ''))[:10],
                    'building': row.get('building', ''),
                    'level': row.get('level', ''),
                    'trade': row.get('trade_name', ''),
                    'company': row.get('company_name', ''),
                    'result': row.get('inspection_result', ''),
                }

        return {'status': 'not_found', 'inspection_id': inspection_id}


###############################################################################
# MAIN SUMMARY GENERATOR
###############################################################################

def generate_period_summary(
    year: int,
    month: int,
    sections: list = None,
    top_n: int = DEFAULT_TOP_N,
    detail_level: str = 'summary',
) -> dict:
    """
    Generate a comprehensive summary for a period.

    Args:
        year: Year (e.g., 2024)
        month: Month (1-12)
        sections: List of sections to include ('labor', 'schedule', 'quality', 'narratives', 'all')
        top_n: Maximum items per category
        detail_level: 'summary' or 'detail'

    Returns:
        dict with all requested summaries
    """
    if sections is None:
        sections = ['all']

    if 'all' in sections:
        sections = ['labor', 'schedule', 'quality', 'narratives']

    loader = PeriodDataLoader(year, month)

    result = {
        'period': f"{year}-{month:02d}",
        'generated_at': datetime.now().isoformat(),
        'detail_level': detail_level,
        'top_n': top_n,
    }

    if 'labor' in sections:
        result['labor'] = {
            'by_trade': LaborSummary.by_trade(loader, top_n),
            'by_company': LaborSummary.by_company(loader, top_n),
        }

    if 'schedule' in sections:
        result['schedule'] = {
            'overview': ScheduleSummary.slippage_overview(loader),
            'by_category': ScheduleSummary.delay_by_category(loader),
        }
        if detail_level == 'detail':
            result['schedule']['delay_drivers'] = ScheduleSummary.delay_drivers(loader, top_n)

    if 'quality' in sections:
        result['quality'] = {
            'overview': QualitySummary.overview(loader),
            'by_trade': QualitySummary.by_trade(loader, top_n),
            'by_location': QualitySummary.by_location(loader, top_n),
        }
        if detail_level == 'detail':
            result['quality']['failures'] = QualitySummary.failures(loader, top_n)

    if 'narratives' in sections:
        result['narratives'] = {
            'overview': NarrativeSummary.overview(loader),
        }
        if detail_level == 'detail':
            result['narratives']['delay_claims'] = NarrativeSummary.delay_claims(loader, top_n)
            result['narratives']['by_category'] = NarrativeSummary.by_category(loader, top_n=5)

    # Estimate token count
    json_str = json.dumps(result, default=str)
    result['_meta'] = {
        'char_count': len(json_str),
        'estimated_tokens': len(json_str) // 4,  # Rough estimate
    }

    return result


###############################################################################
# CLI
###############################################################################

def format_markdown(summary: dict) -> str:
    """Format summary as markdown for human readability."""
    lines = []

    period = summary.get('period', 'Unknown')
    lines.append(f"# Period Summary: {period}")
    lines.append("")

    # Labor section
    if 'labor' in summary:
        lines.append("## Labor Hours")
        lines.append("")

        by_trade = summary['labor'].get('by_trade', {})
        if by_trade.get('status') == 'ok':
            lines.append(f"**Total Hours:** {by_trade.get('total_hours', 0):,}")
            lines.append(f"**Total Headcount:** {by_trade.get('total_headcount', 0):,}")
            lines.append(f"**Companies:** {by_trade.get('total_companies', 0)}")
            lines.append("")
            lines.append("### By Trade (Top {})".format(by_trade.get('trades_shown', 0)))
            lines.append("")
            lines.append("| Trade | Hours | % | Headcount |")
            lines.append("|-------|------:|--:|----------:|")
            for t in by_trade.get('by_trade', []):
                lines.append(f"| {t.get('trade_name', 'Unknown')[:30]} | {t.get('total_hours', 0):,.0f} | {t.get('pct_of_total', 0):.1f}% | {t.get('headcount', 0)} |")
            lines.append("")

    # Schedule section
    if 'schedule' in summary:
        lines.append("## Schedule")
        lines.append("")

        overview = summary['schedule'].get('overview', {})
        if overview.get('status') == 'ok':
            lines.append(f"**Project Slippage:** {overview.get('slippage_days', 0)} days")
            lines.append(f"**Previous Finish:** {overview.get('project_finish_prev', '')[:10]}")
            lines.append(f"**Current Finish:** {overview.get('project_finish_curr', '')[:10]}")
            lines.append("")

        by_cat = summary['schedule'].get('by_category', {})
        if by_cat.get('status') == 'ok':
            lines.append("### Delay Categories")
            lines.append("")
            lines.append("| Category | Tasks | Avg Delay | Total Delay |")
            lines.append("|----------|------:|----------:|------------:|")
            for c in by_cat.get('by_category', []):
                lines.append(f"| {c.get('delay_category', '')} | {c.get('task_count', 0)} | {c.get('avg_own_delay', 0):.1f}d | {c.get('total_own_delay', 0):.0f}d |")
            lines.append("")

        # Detail: delay drivers
        drivers = summary['schedule'].get('delay_drivers', {})
        if drivers.get('status') == 'ok':
            lines.append("### Top Delay Drivers (Driving Path)")
            lines.append("")
            driving = drivers.get('driving_path_delayers', [])
            if driving:
                lines.append("| Task | Own Delay | Inherited | Status |")
                lines.append("|------|----------:|----------:|--------|")
                for t in driving[:10]:
                    lines.append(f"| {t.get('task_code', '')[:20]} | {t.get('own_delay_days', 0):.1f}d | {t.get('inherited_delay_days', 0):.1f}d | {t.get('status', '')} |")
                lines.append("")
            else:
                lines.append("*No driving path delayers*")
                lines.append("")

    # Quality section
    if 'quality' in summary:
        lines.append("## Quality")
        lines.append("")

        overview = summary['quality'].get('overview', {})
        if overview.get('status') == 'ok':
            lines.append(f"**Total Inspections:** {overview.get('total_inspections', 0)}")

            raba = overview.get('raba')
            if raba:
                pass_rate = raba.get('pass_rate_pct')
                fail_rate = raba.get('fail_rate_pct')
                rate_str = f"{pass_rate}% pass, {fail_rate}% fail" if pass_rate is not None else "N/A"
                lines.append(f"- RABA: {raba.get('total_inspections', 0)} inspections ({rate_str})")

            psi = overview.get('psi')
            if psi:
                pass_rate = psi.get('pass_rate_pct')
                fail_rate = psi.get('fail_rate_pct')
                rate_str = f"{pass_rate}% pass, {fail_rate}% fail" if pass_rate is not None else "N/A"
                lines.append(f"- PSI: {psi.get('total_inspections', 0)} inspections ({rate_str})")
            lines.append("")

        # Add by trade table
        by_trade = summary['quality'].get('by_trade', {})
        if by_trade.get('status') == 'ok' and by_trade.get('by_trade'):
            lines.append("### Inspections by Trade")
            lines.append("")
            lines.append("| Source | Trade | Inspections | Pass Rate |")
            lines.append("|--------|-------|------------:|----------:|")
            for t in by_trade.get('by_trade', []):
                lines.append(f"| {t.get('source', '')} | {t.get('trade', '')[:25]} | {t.get('inspections', 0)} | {t.get('pass_rate', 0):.1f}% |")
            lines.append("")

        # Detail: failures
        failures = summary['quality'].get('failures', {})
        if failures.get('status') == 'ok' and failures.get('failures'):
            lines.append(f"### Failed Inspections ({failures.get('total_failures', 0)} total)")
            lines.append("")
            lines.append("| Source | Date | Building | Level | Trade | Failure Reason |")
            lines.append("|--------|------|----------|-------|-------|----------------|")
            for f in failures.get('failures', [])[:15]:
                reason = f.get('failure_reason', '')[:30] if f.get('failure_reason') else ''
                lines.append(f"| {f.get('source', '')} | {f.get('date', '')} | {f.get('building', '')} | {f.get('level', '')} | {f.get('trade', '')[:15]} | {reason} |")
            lines.append("")

    # Narratives section
    if 'narratives' in summary:
        lines.append("## Narrative Statements")
        lines.append("")

        overview = summary['narratives'].get('overview', {})
        if overview.get('status') == 'ok':
            lines.append(f"**Total Statements:** {overview.get('total_statements', 0)}")
            lines.append(f"**Statements with Impact Claims:** {overview.get('statements_with_impact', 0)}")
            lines.append(f"**Total Impact Days Claimed:** {overview.get('total_impact_days_claimed', 0)}")
            lines.append("")

            # By category
            by_cat = overview.get('by_category', {})
            if by_cat:
                lines.append("### By Category")
                lines.append("")
                lines.append("| Category | Count |")
                lines.append("|----------|------:|")
                for cat, count in sorted(by_cat.items(), key=lambda x: -x[1]):
                    lines.append(f"| {cat} | {count} |")
                lines.append("")

            # Parties
            parties = overview.get('parties', [])
            if parties:
                lines.append(f"**Parties Mentioned:** {', '.join(parties[:8])}")
                lines.append("")

        # Detail: delay claims
        delay_claims = summary['narratives'].get('delay_claims', {})
        if delay_claims.get('status') == 'ok' and delay_claims.get('statements'):
            lines.append("### Delay-Related Statements")
            lines.append("")
            for stmt in delay_claims.get('statements', [])[:10]:
                impact = f" ({stmt.get('impact_days')} days)" if stmt.get('impact_days') else ""
                parties = ', '.join(stmt.get('parties', [])[:3])
                lines.append(f"- **[{stmt.get('category', '').upper()}]** {stmt.get('text', '')[:150]}...{impact}")
                if parties:
                    lines.append(f"  - Parties: {parties}")
            lines.append("")

    # Meta
    meta = summary.get('_meta', {})
    lines.append("---")
    lines.append(f"*Estimated tokens: {meta.get('estimated_tokens', 'N/A'):,}*")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='Generate period summary for LLM analysis')
    parser.add_argument('period', nargs='?', help='Period in YYYY-MM format (e.g., 2024-06)')
    parser.add_argument('--section', '-s', action='append', dest='sections',
                        choices=['labor', 'schedule', 'quality', 'narratives', 'all'],
                        help='Sections to include (can specify multiple)')
    parser.add_argument('--top-n', '-n', type=int, default=DEFAULT_TOP_N,
                        help=f'Max items per category (default: {DEFAULT_TOP_N})')
    parser.add_argument('--detail', '-d', action='store_true',
                        help='Include detailed breakdowns')
    parser.add_argument('--format', '-f', choices=['json', 'markdown'], default='markdown',
                        help='Output format (default: markdown)')
    parser.add_argument('--output', '-o', type=str,
                        help='Output file path (default: stdout)')

    args = parser.parse_args()

    if not args.period:
        parser.print_help()
        sys.exit(1)

    # Parse period
    try:
        year, month = args.period.split('-')
        year = int(year)
        month = int(month)
    except ValueError:
        print(f"Error: Invalid period format '{args.period}'. Use YYYY-MM.", file=sys.stderr)
        sys.exit(1)

    # Generate summary
    summary = generate_period_summary(
        year=year,
        month=month,
        sections=args.sections,
        top_n=min(args.top_n, MAX_TOP_N),
        detail_level='detail' if args.detail else 'summary',
    )

    # Format output
    if args.format == 'json':
        output = json.dumps(summary, indent=2, default=str)
    else:
        output = format_markdown(summary)

    # Write output
    if args.output:
        Path(args.output).write_text(output)
        print(f"Written to {args.output}")
    else:
        print(output)


if __name__ == '__main__':
    main()
