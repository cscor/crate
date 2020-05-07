#!/usr/bin/env python3

"""
Use to apply patches from ES upstream with:

    git apply --reject \
        <(curl -L https://github.com/elastic/elasticsearch/pull/<NUMBER>.diff | ./devs/tools/adapt-es-path.py)
"""

import sys


def main():
    for line in sys.stdin:
        sys.stdout.write(
            line
            .replace('diff --git a/test/framework', 'diff --git a/es/es-testing/')
            .replace('--- a/test/framework', '--- a/es/es-testing/')
            .replace('+++ b/test/framework', '+++ b/es/es-testing/')
        )


if __name__ == "__main__":
    main()
