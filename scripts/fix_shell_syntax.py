#!/usr/bin/env python3
"""Fix shell scripts to use [[ instead of ["""

import re
from pathlib import Path

def fix_shell_script(content: str) -> str:
    """Replace [ ] with [[ ]] in shell conditionals."""
    lines = content.split('\n')
    fixed_lines = []
    
    for line in lines:
        # Match patterns like: if [ ... ]; then, elif [ ... ]; then, while [ ... ]; do
        # Also handle multi-line conditionals with \
        
        # Replace standalone [ ] with [[ ]]
        # Match: [ followed by space, content, space, ]
        fixed_line = re.sub(r'\[ ([^]]+) \]', r'[[ \1 ]]', line)
        fixed_lines.append(fixed_line)
    
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
        fixed = fix_shell_script(content)
        path.write_text(fixed)
        print(f"Fixed {filepath}")
    
    print("Done!")


if __name__ == '__main__':
    main()
