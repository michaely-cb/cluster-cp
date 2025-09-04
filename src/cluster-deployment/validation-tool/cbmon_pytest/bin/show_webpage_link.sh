#!/bin/bash

pgrep -f "python -m http.server 9000" | xargs kill -9 > /dev/null 2>&1
python -m http.server 9000 > /dev/null 2>&1 &

echo "Tests finished running..."
echo "=============================================================="
echo ""
echo "If you want to view the 'Reports' web-page, please view it as:"
echo "     http://$HOSTNAME:9000/report.html"
echo ""
echo "=============================================================="
