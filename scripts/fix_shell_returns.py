#!/usr/bin/env python3
"""Add explicit return 0 at the end of shell functions."""

import re
from pathlib import Path

def add_explicit_returns(content: str) -> str:
    """Add 'return 0' at the end of shell functions."""
    lines = content.split('\n')
    fixed_lines = []
    in_function = False
    function_indent = ""
    brace_count = 0
    
    for i, line in enumerate(lines):
        fixed_lines.append(line)
        
        # Check if this is a function definition
        if re.match(r'^(\w+)\s*\(\)\s*\{', line.strip()):
            in_function = True
            # Get the indentation of the function
            function_indent = ""
            continue
        
        if in_function:
            # Count braces to track function end
            brace_count += line.count('{') - line.count('}')
            
            # Check if this is the end of the function (closing brace)
            if line.strip() == '}' and brace_count == 0:
                # Check if previous line has return statement
                prev_line = fixed_lines[-2] if len(fixed_lines) > 1 else ""
                if 'return' not in prev_line and 'exit' not in prev_line:
                    # Insert return 0 before the closing brace
                    fixed_lines.insert(-1, '    return 0')
                in_function = False
                brace_count = 0
    
    return '\n'.join(fixed_lines)


def main():
    files = [
        'scripts/setup_local_env/start.sh',
        'scripts/setup_local_env/stop.sh', 
        'scripts/setup_local_env/logs.sh',
        'scripts/validate_setup.sh'
    ]
    
    for filepath in files:
        path = Path(filepath)
        if not path.exists():
            print(f"Skipping {filepath} - not found")
            continue
            
        content = path.read_text()
        fixed = add_explicit_returns(content)
        path.write_text(fixed)
        print(f"Fixed {filepath}")
    
    print("Done!")


if __name__ == '__main__':
    main()
