"""
Calls do_backup with all *.mbox files recursively
"""

import subprocess
import os
import sys


def main():
    for folder, _, files_list in os.walk(sys.argv[1]):
        for filename in files_list:
            if not filename.lower().endswith(".mbox"):
                continue
            full_path = os.path.join(folder, filename)
            print("processing:", full_path)
            subprocess.call(
                ["python3", "do_backup.py", "--input", full_path, *sys.argv[2:]],
                stderr=sys.stderr,
                stdout=sys.stdout
            )


if __name__ == "__main__":
    main()
