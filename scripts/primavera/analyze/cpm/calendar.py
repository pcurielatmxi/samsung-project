"""
P6 Calendar Parser and Date Arithmetic.

Parses P6's complex calendar format and provides work-day-aware date calculations.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime, date, time, timedelta
from typing import Optional


# Excel epoch: December 30, 1899
EXCEL_EPOCH = date(1899, 12, 30)


def excel_serial_to_date(serial: int) -> date:
    """Convert Excel serial date number to Python date."""
    return EXCEL_EPOCH + timedelta(days=serial)


def date_to_excel_serial(dt: date) -> int:
    """Convert Python date to Excel serial date number."""
    return (dt - EXCEL_EPOCH).days


@dataclass
class WorkPeriod:
    """A work period within a day (e.g., 8:00-12:00)."""
    start: time
    finish: time

    def hours(self) -> float:
        """Calculate hours in this work period."""
        start_minutes = self.start.hour * 60 + self.start.minute
        finish_minutes = self.finish.hour * 60 + self.finish.minute
        return (finish_minutes - start_minutes) / 60.0

    def contains_time(self, t: time) -> bool:
        """Check if a time falls within this work period."""
        return self.start <= t < self.finish


@dataclass
class P6Calendar:
    """
    P6 Calendar with work hours and exceptions.

    Handles P6's complex calendar format including:
    - Work days and hours per day of week
    - Exception dates (holidays, modified work days)
    - Date arithmetic respecting work hours
    """

    clndr_id: str
    clndr_name: str = ""
    hours_per_day: float = 8.0

    # Day of week work periods (1=Sunday, 2=Monday, ..., 7=Saturday)
    work_week: dict[int, list[WorkPeriod]] = field(default_factory=dict)

    # Exception dates: date -> work periods (empty list = holiday)
    exceptions: dict[date, list[WorkPeriod]] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize default work week if not set."""
        if not self.work_week:
            # Default: Mon-Fri, 8am-12pm, 1pm-5pm
            default_periods = [
                WorkPeriod(time(8, 0), time(12, 0)),
                WorkPeriod(time(13, 0), time(17, 0)),
            ]
            for day in range(2, 7):  # Monday (2) through Friday (6)
                self.work_week[day] = default_periods.copy()
            # No work on weekends
            self.work_week[1] = []  # Sunday
            self.work_week[7] = []  # Saturday

    @classmethod
    def from_p6_data(cls, clndr_id: str, clndr_data: str, day_hr_cnt: float = 8.0,
                     clndr_name: str = "") -> 'P6Calendar':
        """
        Parse P6 calendar data string into a P6Calendar object.

        Args:
            clndr_id: Calendar ID
            clndr_data: P6's nested calendar format string
            day_hr_cnt: Default hours per day
            clndr_name: Calendar name
        """
        calendar = cls(
            clndr_id=clndr_id,
            clndr_name=clndr_name,
            hours_per_day=day_hr_cnt,
            work_week={},
            exceptions={},
        )

        if clndr_data:
            calendar._parse_clndr_data(clndr_data)

        return calendar

    def _parse_clndr_data(self, data: str) -> None:
        """Parse P6's nested calendar format."""
        # Extract DaysOfWeek section using balanced parentheses
        dow_data = self._extract_section(data, 'DaysOfWeek()(')
        if dow_data:
            self._parse_days_of_week(dow_data)

        # Extract Exceptions section using balanced parentheses
        exc_data = self._extract_section(data, 'Exceptions()(')
        if exc_data:
            self._parse_exceptions(exc_data)

    def _extract_section(self, data: str, marker: str) -> Optional[str]:
        """Extract a section using balanced parentheses counting."""
        start_idx = data.find(marker)
        if start_idx < 0:
            return None

        start_idx += len(marker)
        depth = 1
        end_idx = start_idx

        while depth > 0 and end_idx < len(data):
            if data[end_idx] == '(':
                depth += 1
            elif data[end_idx] == ')':
                depth -= 1
            end_idx += 1

        # Don't include the final closing parenthesis
        return data[start_idx:end_idx - 1]

    def _parse_days_of_week(self, dow_data: str) -> None:
        """Parse DaysOfWeek section."""
        # Pattern: (0||N()(work_periods)) where N is day number 1-7
        # Day with no work: (0||1()())
        # Day with work: (0||2()( (0||0(s|08:00|f|16:00)()) ))

        # Find each day entry using balanced parentheses (handles nested structure)
        # Match (0||N() where N is 1-7 (day numbers, not work period sequence numbers)
        for match in re.finditer(r'\(0\|\|([1-7])\(\)', dow_data):
            day_num = int(match.group(1))
            start = match.start()

            # Count parens to find the end of this day entry
            depth = 0
            end = start
            for i in range(start, len(dow_data)):
                if dow_data[i] == '(':
                    depth += 1
                elif dow_data[i] == ')':
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        break

            entry = dow_data[start:end]

            # Extract work periods: strip the (0||N()( prefix and )) suffix
            inner_start = entry.find('()(') + 3
            inner_end = len(entry) - 2  # remove last ))
            periods_data = entry[inner_start:inner_end].strip()

            if not periods_data:
                # No work on this day
                self.work_week[day_num] = []
            else:
                # Parse work periods
                self.work_week[day_num] = self._parse_work_periods(periods_data)

    def _parse_exceptions(self, exc_data: str) -> None:
        """Parse Exceptions section."""
        # Pattern: (0||N(d|SERIAL)(work_periods))
        # Holiday: (0||0(d|44525)())
        # Modified day: (0||1(d|44578)( (0||0(s|08:00|f|16:00)()) ))

        # Find all exception definitions
        exc_pattern = r'\(0\|\|\d+\(d\|(\d+)\)\((.*?)\)\)'
        for match in re.finditer(exc_pattern, exc_data, re.DOTALL):
            serial = int(match.group(1))
            periods_data = match.group(2).strip()

            exc_date = excel_serial_to_date(serial)

            if not periods_data:
                # Holiday - no work
                self.exceptions[exc_date] = []
            else:
                # Modified work day
                self.exceptions[exc_date] = self._parse_work_periods(periods_data)

    def _parse_work_periods(self, periods_data: str) -> list[WorkPeriod]:
        """Parse work period definitions."""
        periods = []

        # Pattern 1: (0||N(s|HH:MM|f|HH:MM)()) - start then finish
        period_pattern1 = r'\(0\|\|\d+\(s\|(\d{1,2}:\d{2})\|f\|(\d{1,2}:\d{2})\)\(\)\)'
        for match in re.finditer(period_pattern1, periods_data):
            start_str = match.group(1)
            finish_str = match.group(2)
            # Normalize time format (8:00 -> 08:00)
            start = time.fromisoformat(start_str.zfill(5))
            finish = time.fromisoformat(finish_str.zfill(5))
            periods.append(WorkPeriod(start, finish))

        # Pattern 2: (0||N(f|HH:MM|s|HH:MM)()) - finish then start (alternate format)
        period_pattern2 = r'\(0\|\|\d+\(f\|(\d{1,2}:\d{2})\|s\|(\d{1,2}:\d{2})\)\(\)\)'
        for match in re.finditer(period_pattern2, periods_data):
            finish_str = match.group(1)
            start_str = match.group(2)
            # Normalize time format (8:00 -> 08:00)
            start = time.fromisoformat(start_str.zfill(5))
            finish = time.fromisoformat(finish_str.zfill(5))
            periods.append(WorkPeriod(start, finish))

        # Sort periods by start time
        periods.sort(key=lambda p: p.start)
        return periods

    def get_work_periods(self, dt: date) -> list[WorkPeriod]:
        """Get work periods for a specific date."""
        # Check exceptions first
        if dt in self.exceptions:
            return self.exceptions[dt]

        # Use day of week (P6: 1=Sunday, Python: 0=Monday)
        # Convert: Python weekday() 0-6 (Mon-Sun) -> P6 1-7 (Sun-Sat)
        python_weekday = dt.weekday()
        p6_day = (python_weekday + 2) % 7
        if p6_day == 0:
            p6_day = 7

        return self.work_week.get(p6_day, [])

    def is_work_day(self, dt: date) -> bool:
        """Check if a date is a work day."""
        return len(self.get_work_periods(dt)) > 0

    def get_work_hours(self, dt: date) -> float:
        """Get total work hours available on a specific date."""
        periods = self.get_work_periods(dt)
        hours = sum(p.hours() for p in periods)

        # Fallback: if periods exist but have 0 hours (e.g., 00:00-00:00 curing calendars),
        # use hours_per_day as the daily work hours
        if periods and hours == 0:
            return self.hours_per_day

        return hours

    def add_work_hours(self, start: datetime, hours: float) -> datetime:
        """
        Add work hours to a datetime, respecting calendar.

        If hours is 0, returns start unchanged.
        If start is not during work hours, advances to next work period first.
        """
        if hours <= 0:
            return start

        current = start
        remaining = hours

        # If starting outside work hours, move to next work period
        current = self.advance_to_work_time(current)

        max_iterations = 365 * 10  # Safety limit
        iteration = 0

        while remaining > 0 and iteration < max_iterations:
            iteration += 1
            periods = self.get_work_periods(current.date())
            daily_hours = self.get_work_hours(current.date())

            # Handle calendars with 0-hour periods (e.g., curing calendars)
            # Use hours_per_day spread across a standard 8-5 day
            if periods and daily_hours > 0 and all(p.hours() == 0 for p in periods):
                # Special handling: treat as continuous day with hours_per_day
                if remaining >= daily_hours:
                    remaining -= daily_hours
                    current = datetime.combine(current.date() + timedelta(days=1), time(8, 0))
                else:
                    # Partial day - spread hours_per_day across 8 hours (8am-4pm for 8hr day)
                    hours_elapsed = remaining
                    current = datetime.combine(current.date(), time(8, 0)) + timedelta(hours=hours_elapsed)
                    remaining = 0
                continue

            for period in periods:
                period_start = datetime.combine(current.date(), period.start)
                period_end = datetime.combine(current.date(), period.finish)

                # Skip periods we've already passed
                if current >= period_end:
                    continue

                # Start from current time or period start, whichever is later
                work_start = max(current, period_start)
                available_hours = (period_end - work_start).total_seconds() / 3600

                if available_hours >= remaining:
                    # We finish within this period
                    return work_start + timedelta(hours=remaining)
                else:
                    # Use all available hours in this period
                    remaining -= available_hours
                    current = period_end

            # Move to next day
            current = datetime.combine(current.date() + timedelta(days=1), time(0, 0))
            current = self.advance_to_work_time(current)

        return current

    def subtract_work_hours(self, end: datetime, hours: float) -> datetime:
        """
        Subtract work hours from a datetime, respecting calendar.

        If hours is 0, returns end unchanged.
        """
        if hours <= 0:
            return end

        current = end
        remaining = hours

        # If ending outside work hours, move back to previous work period
        current = self._retreat_to_work_time(current)

        max_iterations = 365 * 10  # Safety limit
        iteration = 0

        while remaining > 0 and iteration < max_iterations:
            iteration += 1
            periods = self.get_work_periods(current.date())
            daily_hours = self.get_work_hours(current.date())

            # Handle calendars with 0-hour periods (e.g., curing calendars)
            if periods and daily_hours > 0 and all(p.hours() == 0 for p in periods):
                # Special handling: treat as continuous day with hours_per_day
                if remaining >= daily_hours:
                    remaining -= daily_hours
                    current = datetime.combine(current.date() - timedelta(days=1), time(17, 0))
                else:
                    hours_remaining = remaining
                    current = datetime.combine(current.date(), time(17, 0)) - timedelta(hours=hours_remaining)
                    remaining = 0
                continue

            # Process periods in reverse order
            for period in reversed(periods):
                period_start = datetime.combine(current.date(), period.start)
                period_end = datetime.combine(current.date(), period.finish)

                # Skip periods after current time
                if current <= period_start:
                    continue

                # End at current time or period end, whichever is earlier
                work_end = min(current, period_end)
                available_hours = (work_end - period_start).total_seconds() / 3600

                if available_hours >= remaining:
                    # We finish within this period
                    return work_end - timedelta(hours=remaining)
                else:
                    # Use all available hours in this period
                    remaining -= available_hours
                    current = period_start

            # Move to previous day
            current = datetime.combine(current.date() - timedelta(days=1), time(23, 59, 59))
            current = self._retreat_to_work_time(current)

        return current

    def work_hours_between(self, start: datetime, end: datetime) -> float:
        """Calculate work hours between two datetimes."""
        if end <= start:
            return 0.0

        total_hours = 0.0
        current_date = start.date()

        while current_date <= end.date():
            periods = self.get_work_periods(current_date)
            daily_hours = self.get_work_hours(current_date)

            # Handle calendars with 0-hour periods (e.g., curing calendars)
            if periods and daily_hours > 0 and all(p.hours() == 0 for p in periods):
                # For curing calendars, count full days of hours_per_day
                if current_date == start.date() and current_date == end.date():
                    # Same day - fraction based on elapsed time
                    elapsed = (end - start).total_seconds() / 3600
                    total_hours += min(elapsed, daily_hours)
                elif current_date == start.date():
                    # Partial first day
                    total_hours += daily_hours * 0.5  # Approximate
                elif current_date == end.date():
                    # Partial last day
                    total_hours += daily_hours * 0.5  # Approximate
                else:
                    # Full day
                    total_hours += daily_hours
                current_date += timedelta(days=1)
                continue

            for period in periods:
                period_start = datetime.combine(current_date, period.start)
                period_end = datetime.combine(current_date, period.finish)

                # Calculate overlap with [start, end]
                work_start = max(start, period_start)
                work_end = min(end, period_end)

                if work_start < work_end:
                    total_hours += (work_end - work_start).total_seconds() / 3600

            current_date += timedelta(days=1)

        return total_hours

    def advance_to_work_time(self, dt: datetime) -> datetime:
        """Advance datetime to next work time if not already in one."""
        max_days = 365  # Safety limit

        for _ in range(max_days):
            periods = self.get_work_periods(dt.date())
            daily_hours = self.get_work_hours(dt.date())

            # Handle calendars with 0-hour periods (e.g., curing calendars)
            if periods and daily_hours > 0 and all(p.hours() == 0 for p in periods):
                # Treat as if work starts at 8am
                if dt.time() < time(8, 0):
                    return datetime.combine(dt.date(), time(8, 0))
                return dt

            for period in periods:
                period_start = datetime.combine(dt.date(), period.start)
                period_end = datetime.combine(dt.date(), period.finish)

                if dt < period_start:
                    # Before this period starts
                    return period_start
                elif dt < period_end:
                    # Within this period
                    return dt

            # No more work periods today, move to next day
            dt = datetime.combine(dt.date() + timedelta(days=1), time(0, 0))

        raise ValueError(f"Could not find work time within {max_days} days")

    def _retreat_to_work_time(self, dt: datetime) -> datetime:
        """Retreat datetime to previous work time if not already in one."""
        max_days = 365  # Safety limit

        for _ in range(max_days):
            periods = self.get_work_periods(dt.date())
            daily_hours = self.get_work_hours(dt.date())

            # Handle calendars with 0-hour periods (e.g., curing calendars)
            if periods and daily_hours > 0 and all(p.hours() == 0 for p in periods):
                # Treat as if work ends at 5pm
                if dt.time() > time(17, 0):
                    return datetime.combine(dt.date(), time(17, 0))
                return dt

            for period in reversed(periods):
                period_start = datetime.combine(dt.date(), period.start)
                period_end = datetime.combine(dt.date(), period.finish)

                if dt > period_end:
                    # After this period ends
                    return period_end
                elif dt > period_start:
                    # Within this period
                    return dt

            # No work periods before current time today, move to previous day
            dt = datetime.combine(dt.date() - timedelta(days=1), time(23, 59, 59))

        raise ValueError(f"Could not find work time within {max_days} days")

    def get_end_of_day(self, dt: date) -> Optional[datetime]:
        """Get the end of work day datetime for a date."""
        periods = self.get_work_periods(dt)
        if not periods:
            return None
        last_period = periods[-1]
        return datetime.combine(dt, last_period.finish)

    def get_start_of_day(self, dt: date) -> Optional[datetime]:
        """Get the start of work day datetime for a date."""
        periods = self.get_work_periods(dt)
        if not periods:
            return None
        first_period = periods[0]
        return datetime.combine(dt, first_period.start)

    def get_previous_work_period_end(self, dt: datetime) -> Optional[datetime]:
        """
        Get the end of the previous work period.

        If dt is at the start of a work period (e.g., 07:00 Monday),
        returns the end of the previous work period (e.g., 17:00 Friday).

        Used for FS relationships where predecessor must finish BEFORE
        successor starts.
        """
        max_days = 365  # Safety limit
        current = dt

        for _ in range(max_days):
            periods = self.get_work_periods(current.date())
            daily_hours = self.get_work_hours(current.date())

            # Handle calendars with 0-hour periods (e.g., curing calendars)
            if periods and daily_hours > 0 and all(p.hours() == 0 for p in periods):
                # Move to previous day end
                current = datetime.combine(current.date() - timedelta(days=1), time(23, 59, 59))
                continue

            for period in reversed(periods):
                period_start = datetime.combine(current.date(), period.start)
                period_end = datetime.combine(current.date(), period.finish)

                if current > period_end:
                    # After this period - this is the previous work period end
                    return period_end
                elif current > period_start:
                    # Within this period - need to go to previous period
                    # Check if there's an earlier period today
                    period_idx = periods.index(period)
                    if period_idx > 0:
                        prev_period = periods[period_idx - 1]
                        return datetime.combine(current.date(), prev_period.finish)
                    # No earlier period today, go to previous day
                    current = datetime.combine(current.date() - timedelta(days=1), time(23, 59, 59))
                    break
                elif current == period_start:
                    # At exact start of period - need previous period end
                    period_idx = periods.index(period)
                    if period_idx > 0:
                        prev_period = periods[period_idx - 1]
                        return datetime.combine(current.date(), prev_period.finish)
                    # No earlier period today, go to previous day
                    current = datetime.combine(current.date() - timedelta(days=1), time(23, 59, 59))
                    break
            else:
                # No work periods today or before current time, go to previous day
                current = datetime.combine(current.date() - timedelta(days=1), time(23, 59, 59))

        return None

    def count_work_days(self, start: date, end: date) -> int:
        """Count work days between two dates (inclusive)."""
        count = 0
        current = start
        while current <= end:
            if self.is_work_day(current):
                count += 1
            current += timedelta(days=1)
        return count

    def add_work_days(self, start: date, days: int) -> date:
        """Add work days to a date."""
        if days <= 0:
            return start

        current = start
        remaining = days

        while remaining > 0:
            current += timedelta(days=1)
            if self.is_work_day(current):
                remaining -= 1

        return current

    def __repr__(self) -> str:
        work_days = sum(1 for d in range(1, 8) if self.work_week.get(d))
        return f"P6Calendar({self.clndr_id}, {work_days}-day week, {len(self.exceptions)} exceptions)"
