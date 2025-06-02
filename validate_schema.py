#!/usr/bin/env python3
"""
Validate SQL schema syntax without requiring a database connection.
"""

import re
import sys


def validate_sql_schema(file_path="sql/schema.sql"):
    """Validate SQL schema file for common syntax issues."""

    print(f"Validating schema file: {file_path}")

    try:
        with open(file_path, 'r') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"ERROR: Schema file not found: {file_path}")
        return False

    errors = []
    warnings = []

    # Check for basic SQL syntax issues
    lines = content.split('\n')
    in_function = False
    function_start = 0
    dollar_quote_count = 0

    for i, line in enumerate(lines, 1):
        line_stripped = line.strip()

        # Skip empty lines and comments
        if not line_stripped or line_stripped.startswith('--'):
            continue

        # Check for function definitions
        if 'CREATE OR REPLACE FUNCTION' in line_stripped.upper():
            in_function = True
            function_start = i
            dollar_quote_count = 0

        # Count dollar quotes
        if '$$' in line:
            dollar_quote_count += line.count('$$')

        # Check if function ends
        if in_function and 'LANGUAGE' in line_stripped.upper() and line_stripped.endswith(';'):
            if dollar_quote_count % 2 != 0:
                errors.append(
                    f"Line {function_start}-{i}: Unmatched dollar quotes in function definition")
            in_function = False

        # Check for common syntax issues
        if line_stripped.endswith(',;'):
            errors.append(f"Line {i}: Extra comma before semicolon")

        # Check for unmatched parentheses (simple check)
        open_parens = line.count('(')
        close_parens = line.count(')')
        if abs(open_parens - close_parens) > 3:  # Allow some flexibility for multi-line statements
            warnings.append(f"Line {i}: Potentially unmatched parentheses")

    # Check for overall structure
    required_elements = [
        ('CREATE TABLE', 'teams'),
        ('CREATE TABLE', 'gameweeks'),
        ('CREATE TABLE', 'players'),
        ('CREATE TABLE', 'fixtures'),
        ('CREATE INDEX', 'idx_'),
        ('CREATE OR REPLACE FUNCTION', 'update_updated_at_column'),
        ('CREATE TRIGGER', 'update_teams_updated_at'),
    ]

    content_upper = content.upper()
    for element_type, element_name in required_elements:
        if element_type not in content_upper:
            errors.append(f"Missing {element_type} statements")
        elif element_name.upper() not in content_upper:
            warnings.append(
                f"Expected {element_name} in {element_type} not found")

    # Validate function syntax more specifically
    function_pattern = r'CREATE OR REPLACE FUNCTION.*?\$\$.*?\$\$ LANGUAGE'
    functions = re.findall(function_pattern, content,
                           re.DOTALL | re.IGNORECASE)

    for func in functions:
        if func.count('$$') != 2:
            errors.append(
                "Function definition has incorrect number of dollar quotes")
        if 'BEGIN' not in func.upper() or 'END' not in func.upper():
            errors.append("Function missing BEGIN/END block")
        if 'RETURN' not in func.upper():
            warnings.append("Function appears to be missing RETURN statement")

    # Report results
    print(f"\nValidation Results:")
    print(f"  Lines processed: {len(lines)}")
    print(f"  Errors found: {len(errors)}")
    print(f"  Warnings: {len(warnings)}")

    if errors:
        print("\nERRORS:")
        for error in errors:
            print(f"  ❌ {error}")

    if warnings:
        print("\nWARNINGS:")
        for warning in warnings:
            print(f"  ⚠️  {warning}")

    if not errors and not warnings:
        print("  ✅ Schema appears to be syntactically correct!")
    elif not errors:
        print("  ✅ No errors found (warnings can usually be ignored)")

    return len(errors) == 0


if __name__ == "__main__":
    success = validate_sql_schema()
    sys.exit(0 if success else 1)
