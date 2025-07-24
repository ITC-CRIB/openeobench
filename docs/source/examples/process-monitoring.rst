Process Monitoring
==================

This example demonstrates how to monitor OpenEO process implementations and compliance across backends over time.

Setting Up Process Monitoring
------------------------------

Create a comprehensive monitoring system for OpenEO process availability:

.. code-block:: bash

   # Create backend configuration
   cat > backend_monitor.csv << EOF
   name,url
   CDSE,https://openeo.dataspace.copernicus.eu
   VITO,https://openeo.vito.be/openeo/1.1
   GEE,https://earthengine.openeo.org
   EODC,https://openeo-dev.eodc.eu
   EOF

   # Create monitoring directory structure
   mkdir -p process_monitoring/{daily,weekly,monthly}

Daily Process Checks
--------------------

Monitor process availability daily:

.. code-block:: bash
   :caption: daily_process_check.sh

   #!/bin/bash
   
   DATE=$(date +%Y%m%d)
   OUTPUT_DIR="process_monitoring/daily/$DATE"
   mkdir -p "$OUTPUT_DIR"
   
   echo "Running daily process check for $DATE..."
   
   # Check each backend
   while IFS=, read -r name url; do
     [ "$name" = "name" ] && continue  # Skip header
     echo "Checking $name..."
     openeobench process --url "$url" -o "$OUTPUT_DIR/${name}_processes"
   done < backend_monitor.csv
   
   # Generate daily summary
   openeobench process-summary "$OUTPUT_DIR"/ --output "$OUTPUT_DIR/daily_summary.md"
   
   echo "Daily check complete. Results in $OUTPUT_DIR/"

Process Compliance Tracking
----------------------------

Track compliance levels over time:

.. code-block:: python
   :caption: compliance_tracker.py

   #!/usr/bin/env python3
   import csv
   import json
   import os
   from datetime import datetime, timedelta
   from pathlib import Path
   
   def analyze_compliance_trends():
       """Analyze process compliance trends over time."""
       base_dir = Path("process_monitoring/daily")
       
       # Collect data from last 30 days
       compliance_data = {}
       
       for days_back in range(30):
           date = datetime.now() - timedelta(days=days_back)
           date_str = date.strftime("%Y%m%d")
           day_dir = base_dir / date_str
           
           if day_dir.exists():
               # Parse daily summary
               summary_file = day_dir / "daily_summary.csv"
               if summary_file.exists():
                   with open(summary_file, 'r') as f:
                       reader = csv.DictReader(f)
                       for row in reader:
                           backend = row['backend']
                           if backend not in compliance_data:
                               compliance_data[backend] = []
                           
                           compliance_data[backend].append({
                               'date': date_str,
                               'l1_compliance': float(row.get('l1_compliance_rate', 0)),
                               'l2_compliance': float(row.get('l2_compliance_rate', 0)),
                               'l3_compliance': float(row.get('l3_compliance_rate', 0)),
                               'l4_compliance': float(row.get('l4_compliance_rate', 0)),
                           })
       
       # Generate trend report
       with open('compliance_trends.md', 'w') as f:
           f.write("# Process Compliance Trends\\n\\n")
           f.write(f"Report generated: {datetime.now().isoformat()}\\n\\n")
           
           for backend, data in compliance_data.items():
               f.write(f"## {backend}\\n\\n")
               
               if data:
                   latest = data[0]
                   f.write(f"**Current Status:**\\n")
                   f.write(f"- L1 Compliance: {latest['l1_compliance']:.1f}%\\n")
                   f.write(f"- L2 Compliance: {latest['l2_compliance']:.1f}%\\n")
                   f.write(f"- L3 Compliance: {latest['l3_compliance']:.1f}%\\n")
                   f.write(f"- L4 Compliance: {latest['l4_compliance']:.1f}%\\n\\n")
                   
                   # Check for trends
                   if len(data) > 7:
                       week_ago = data[7]
                       f.write(f"**7-day trend:**\\n")
                       for level in ['l1', 'l2', 'l3', 'l4']:
                           key = f"{level}_compliance"
                           change = latest[key] - week_ago[key]
                           trend = "📈" if change > 0 else "📉" if change < 0 else "➡️"
                           f.write(f"- {level.upper()}: {change:+.1f}% {trend}\\n")
                   
                   f.write("\\n")
       
       print("Compliance trends analysis complete: compliance_trends.md")
   
   if __name__ == "__main__":
       analyze_compliance_trends()

Process Change Detection
------------------------

Detect when processes are added or removed:

.. code-block:: bash
   :caption: detect_process_changes.sh

   #!/bin/bash
   
   CURRENT_DATE=$(date +%Y%m%d)
   PREVIOUS_DATE=$(date -d "1 day ago" +%Y%m%d)
   
   CURRENT_DIR="process_monitoring/daily/$CURRENT_DATE"
   PREVIOUS_DIR="process_monitoring/daily/$PREVIOUS_DATE"
   
   if [ ! -d "$PREVIOUS_DIR" ]; then
       echo "No previous day data found for comparison"
       exit 1
   fi
   
   echo "# Process Changes Detected" > process_changes.md
   echo "Date: $CURRENT_DATE" >> process_changes.md
   echo "" >> process_changes.md
   
   # Compare each backend
   for backend in CDSE VITO GEE EODC; do
       current_file="$CURRENT_DIR/${backend}_processes.json"
       previous_file="$PREVIOUS_DIR/${backend}_processes.json"
       
       if [ -f "$current_file" ] && [ -f "$previous_file" ]; then
           echo "## $backend" >> process_changes.md
           echo "" >> process_changes.md
           
           # Extract process lists
           jq -r '.processes[].id' "$current_file" | sort > current_processes.tmp
           jq -r '.processes[].id' "$previous_file" | sort > previous_processes.tmp
           
           # Find additions
           added=$(comm -23 current_processes.tmp previous_processes.tmp)
           if [ -n "$added" ]; then
               echo "### ✅ Added Processes" >> process_changes.md
               echo "$added" | sed 's/^/- /' >> process_changes.md
               echo "" >> process_changes.md
           fi
           
           # Find removals
           removed=$(comm -13 current_processes.tmp previous_processes.tmp)
           if [ -n "$removed" ]; then
               echo "### ❌ Removed Processes" >> process_changes.md
               echo "$removed" | sed 's/^/- /' >> process_changes.md
               echo "" >> process_changes.md
           fi
           
           if [ -z "$added" ] && [ -z "$removed" ]; then
               echo "No changes detected." >> process_changes.md
               echo "" >> process_changes.md
           fi
           
           # Cleanup
           rm -f current_processes.tmp previous_processes.tmp
       fi
   done
   
   echo "Process change detection complete: process_changes.md"

Automated Reporting
-------------------

Create automated weekly and monthly reports:

.. code-block:: bash
   :caption: weekly_report.sh

   #!/bin/bash
   
   WEEK_START=$(date -d "7 days ago" +%Y%m%d)
   WEEK_END=$(date +%Y%m%d)
   REPORT_FILE="process_monitoring/weekly/week_ending_$WEEK_END.md"
   
   mkdir -p "$(dirname "$REPORT_FILE")"
   
   echo "# Weekly Process Monitoring Report" > "$REPORT_FILE"
   echo "Week ending: $WEEK_END" >> "$REPORT_FILE"
   echo "" >> "$REPORT_FILE"
   
   # Aggregate weekly data
   echo "## Summary" >> "$REPORT_FILE"
   echo "" >> "$REPORT_FILE"
   
   # Count daily checks
   check_count=0
   for day_dir in process_monitoring/daily/*/; do
       day=$(basename "$day_dir")
       if [[ "$day" -ge "$WEEK_START" ]] && [[ "$day" -le "$WEEK_END" ]]; then
           ((check_count++))
       fi
   done
   
   echo "- Daily checks performed: $check_count/7" >> "$REPORT_FILE"
   echo "" >> "$REPORT_FILE"
   
   # Include latest compliance summary
   latest_dir=$(ls -1d process_monitoring/daily/*/ | tail -1)
   if [ -d "$latest_dir" ]; then
       echo "## Latest Compliance Status" >> "$REPORT_FILE"
       echo "" >> "$REPORT_FILE"
       if [ -f "${latest_dir}daily_summary.md" ]; then
           tail -n +3 "${latest_dir}daily_summary.md" >> "$REPORT_FILE"
       fi
   fi
   
   # Add trend analysis
   echo "" >> "$REPORT_FILE"
   echo "## Trends" >> "$REPORT_FILE"
   echo "" >> "$REPORT_FILE"
   
   python3 compliance_tracker.py
   if [ -f compliance_trends.md ]; then
       tail -n +3 compliance_trends.md >> "$REPORT_FILE"
   fi
   
   echo "Weekly report generated: $REPORT_FILE"

Process Alerting System
-----------------------

Set up alerts for process compliance issues:

.. code-block:: python
   :caption: process_alerting.py

   #!/usr/bin/env python3
   import csv
   import smtplib
   from email.mime.text import MIMEText
   from email.mime.multipart import MIMEMultipart
   from pathlib import Path
   from datetime import datetime
   
   def check_compliance_alerts():
       """Check for compliance issues and send alerts."""
       
       # Configuration
       ALERT_THRESHOLDS = {
           'l1_compliance_rate': 95.0,  # Alert if L1 < 95%
           'l2_compliance_rate': 80.0,  # Alert if L2 < 80%
           'l3_compliance_rate': 70.0,  # Alert if L3 < 70%
           'l4_compliance_rate': 50.0,  # Alert if L4 < 50%
       }
       
       # Find latest summary
       latest_dir = max(Path("process_monitoring/daily").glob("*/"))
       summary_file = latest_dir / "daily_summary.csv"
       
       if not summary_file.exists():
           return
       
       alerts = []
       
       with open(summary_file, 'r') as f:
           reader = csv.DictReader(f)
           for row in reader:
               backend = row['backend']
               
               for metric, threshold in ALERT_THRESHOLDS.items():
                   value = float(row.get(metric, 0))
                   if value < threshold:
                       level = metric.split('_')[0].upper()
                       alerts.append(f"{backend}: {level} compliance {value:.1f}% (< {threshold}%)")
       
       if alerts:
           send_alert_email(alerts, latest_dir.name)
   
   def send_alert_email(alerts, date):
       """Send alert email."""
       # Email configuration (update with your settings)
       smtp_server = "smtp.example.com"
       smtp_port = 587
       username = "alerts@example.com"
       password = "your_password"
       to_email = "admin@example.com"
       
       subject = f"OpenEO Process Compliance Alert - {date}"
       
       body = f"""
   Process compliance issues detected on {date}:
   
   {chr(10).join(f"• {alert}" for alert in alerts)}
   
   Please review the full report at:
   process_monitoring/daily/{date}/daily_summary.md
   """
       
       msg = MIMEMultipart()
       msg['From'] = username
       msg['To'] = to_email
       msg['Subject'] = subject
       
       msg.attach(MIMEText(body, 'plain'))
       
       try:
           server = smtplib.SMTP(smtp_server, smtp_port)
           server.starttls()
           server.login(username, password)
           text = msg.as_string()
           server.sendmail(username, to_email, text)
           server.quit()
           print(f"Alert email sent for {len(alerts)} compliance issues")
       except Exception as e:
           print(f"Failed to send alert email: {e}")
   
   if __name__ == "__main__":
       check_compliance_alerts()

Cron Setup
----------

Set up automated monitoring with cron:

.. code-block:: bash

   # Edit crontab
   crontab -e
   
   # Add these entries:
   
   # Daily process check at 6 AM
   0 6 * * * /path/to/daily_process_check.sh
   
   # Change detection at 7 AM
   0 7 * * * /path/to/detect_process_changes.sh
   
   # Compliance alerts at 8 AM
   0 8 * * * cd /path/to/openeobench && python3 process_alerting.py
   
   # Weekly report on Mondays at 9 AM
   0 9 * * 1 /path/to/weekly_report.sh

Dashboard Creation
------------------

Create a simple HTML dashboard:

.. code-block:: python
   :caption: create_dashboard.py

   #!/usr/bin/env python3
   import csv
   import json
   from pathlib import Path
   from datetime import datetime
   
   def create_dashboard():
       """Create HTML dashboard from monitoring data."""
       
       # Find latest data
       latest_dir = max(Path("process_monitoring/daily").glob("*/"))
       summary_file = latest_dir / "daily_summary.csv"
       
       if not summary_file.exists():
           print("No summary data found")
           return
       
       # Read compliance data
       backends = []
       with open(summary_file, 'r') as f:
           reader = csv.DictReader(f)
           backends = list(reader)
       
       # Generate HTML
       html = f"""
   <!DOCTYPE html>
   <html>
   <head>
       <title>OpenEO Process Monitoring Dashboard</title>
       <style>
           body {{ font-family: Arial, sans-serif; margin: 20px; }}
           .backend {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; }}
           .metric {{ display: inline-block; margin: 10px; padding: 10px; background: #f5f5f5; }}
           .good {{ background: #d4edda; }}
           .warning {{ background: #fff3cd; }}
           .danger {{ background: #f8d7da; }}
       </style>
   </head>
   <body>
       <h1>OpenEO Process Monitoring Dashboard</h1>
       <p>Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
       
       """
       
       for backend in backends:
           name = backend['backend']
           html += f'<div class="backend"><h2>{name}</h2>'
           
           for level in ['l1', 'l2', 'l3', 'l4']:
               rate = float(backend.get(f'{level}_compliance_rate', 0))
               available = backend.get(f'{level}_available', 'N/A')
               
               css_class = 'good' if rate >= 90 else 'warning' if rate >= 70 else 'danger'
               
               html += f'''
               <div class="metric {css_class}">
                   <strong>{level.upper()}</strong><br>
                   {rate:.1f}% compliance<br>
                   {available} processes
               </div>
               '''
           
           html += '</div>'
       
       html += '</body></html>'
       
       with open('dashboard.html', 'w') as f:
           f.write(html)
       
       print("Dashboard created: dashboard.html")
   
   if __name__ == "__main__":
       create_dashboard()

This monitoring system provides:

1. **Daily automated checks** of all backends
2. **Change detection** when processes are added/removed
3. **Compliance trend analysis** over time
4. **Automated alerting** for compliance issues
5. **Weekly and monthly reports** for stakeholders
6. **Simple dashboard** for quick status overview
