#!/usr/bin/env python3
"""
Test CPM Calculator against P6 stored values.

This script loads a schedule, runs CPM, and compares results with P6's stored values.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from scripts.primavera.analyze.data_loader import (
    load_schedule,
    get_latest_file_id,
    list_schedule_versions,
)
from scripts.primavera.analyze.cpm.engine import CPMEngine
from scripts.primavera.analyze.analysis.critical_path import (
    analyze_critical_path,
    print_critical_path_report,
)


def test_calendar_parsing():
    """Test that calendars are parsed correctly."""
    print("\n" + "=" * 60)
    print("TEST: Calendar Parsing")
    print("=" * 60)

    from scripts.primavera.analyze.data_loader import load_calendars

    file_id = get_latest_file_id()
    calendars = load_calendars(file_id)

    print(f"\nLoaded {len(calendars)} calendars for file_id={file_id}")

    # Show sample calendar
    for cal_id, cal in list(calendars.items())[:3]:
        print(f"\n  Calendar: {cal_id}")
        print(f"    Name: {cal.clndr_name}")
        print(f"    Hours/day: {cal.hours_per_day}")
        print(f"    Work days: ", end="")
        days = []
        day_names = ['', 'Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']
        for d in range(1, 8):
            if cal.work_week.get(d):
                days.append(day_names[d])
        print(", ".join(days))
        print(f"    Exceptions: {len(cal.exceptions)}")

    return True


def test_network_building():
    """Test that network is built correctly."""
    print("\n" + "=" * 60)
    print("TEST: Network Building")
    print("=" * 60)

    file_id = get_latest_file_id()
    network, calendars, project_info = load_schedule(file_id, verbose=True)

    stats = network.get_statistics()
    print(f"\nNetwork Statistics:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    # Validate network
    issues = network.validate()
    if issues:
        print(f"\nValidation issues:")
        for issue in issues[:5]:
            print(f"  - {issue}")
    else:
        print("\nNetwork validation: PASSED")

    return len(issues) == 0 or 'Circular' not in str(issues)


def test_cpm_calculation():
    """Test CPM forward/backward pass."""
    print("\n" + "=" * 60)
    print("TEST: CPM Calculation")
    print("=" * 60)

    file_id = get_latest_file_id()
    network, calendars, project_info = load_schedule(file_id, verbose=True)

    print("\nRunning CPM...")
    data_date = project_info.get('data_date')
    print(f"  Using data_date: {data_date}")
    engine = CPMEngine(network, calendars)
    result = engine.run(data_date=data_date)

    print(f"\nCPM Results:")
    print(f"  Project Start: {result.project_start}")
    print(f"  Project Finish: {result.project_finish}")
    print(f"  Critical Path: {len(result.critical_path)} tasks")
    print(f"  Total Duration: {result.total_duration_hours:.0f} hours "
          f"({result.total_duration_hours/8:.0f} days)")

    # Compare with P6 values
    print("\nComparing with P6 stored values...")
    comparison = engine.compare_with_p6()

    print(f"  Tasks compared: {comparison['total_compared']}")
    print(f"  Early finish match: {comparison['early_finish_match']} "
          f"({comparison['early_finish_match']/max(1,comparison['total_compared'])*100:.1f}%)")
    print(f"  Early finish diff: {comparison['early_finish_diff']}")
    print(f"  Late finish match: {comparison['late_finish_match']}")
    print(f"  Float match: {comparison['float_match']}")
    print(f"  Critical match: {comparison['critical_match']}")

    if comparison['differences']:
        print(f"\nSample differences (first 5):")
        for diff in comparison['differences'][:5]:
            print(f"  {diff['task_name'][:40]}")
            print(f"    Calculated: {diff['calculated']}")
            print(f"    P6:         {diff['p6']}")
            print(f"    Diff:       {diff['diff_hours']:.1f} hours")

    return True


def test_critical_path_analysis():
    """Test critical path analysis."""
    print("\n" + "=" * 60)
    print("TEST: Critical Path Analysis")
    print("=" * 60)

    file_id = get_latest_file_id()
    network, calendars, project_info = load_schedule(file_id, verbose=True)

    data_date = project_info.get('data_date')
    result = analyze_critical_path(network, calendars, data_date=data_date)
    print_critical_path_report(result)

    return True


def main():
    """Run all tests."""
    print("CPM Calculator Test Suite")
    print("=" * 60)

    # List available schedules
    versions = list_schedule_versions()
    print(f"\nAvailable schedule versions: {len(versions)}")
    print(versions.tail(5).to_string())

    tests = [
        ("Calendar Parsing", test_calendar_parsing),
        ("Network Building", test_network_building),
        ("CPM Calculation", test_cpm_calculation),
        ("Critical Path Analysis", test_critical_path_analysis),
    ]

    results = []
    for name, test_func in tests:
        try:
            passed = test_func()
            results.append((name, "PASSED" if passed else "FAILED"))
        except Exception as e:
            print(f"\nERROR in {name}: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, f"ERROR: {e}"))

    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    for name, status in results:
        print(f"  {name}: {status}")

    return all(status == "PASSED" for _, status in results)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
