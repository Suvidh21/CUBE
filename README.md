🧊 CUBE – Service Request Data Management & Visualization Tool

Developed by: Suvidh Mathur (Intern)
Organization: Secure Meters Limited
Deployment Date: July 15, 2025
Contact: suvidhmathur25@gmail.com

🧩 Overview

CUBE is a desktop-based data management and visualization tool built to streamline Service Request (SR) tracking, prioritization, and analysis for the CSS Repair Center.
It automates Turnaround Time (TAT) calculations, enables data filtering and editing, and generates targeted Excel reports to improve service efficiency and decision-making.

🚀 Key Features
🔹 Tkinter Desktop Interface

Import raw Excel files (.xlsb, .xlsx, .xls)

Apply filters for SR Type, SR Status, Product Family, and Date Range

Edit table data directly with validation

Export filtered data to Excel

One-click report generation for:

Unreceived Meters

Unrepaired Meters

Unclosed Repair Lines

Received Not Repaired (by Category)

🔹 Dash Web Dashboard (Auto-refreshes every 15 seconds)

Interactive charts for SR trends, TAT performance, component failures, and backlog ageing

Filter by SR type, status, ageing, product group, and date range

Drill-down integration: click on a chart to auto-filter data in the desktop app

Export dashboard visuals to PDF

⚙️ Technology Stack

Python

Tkinter – Desktop GUI

Pandas, Openpyxl – Excel data handling

Plotly Dash – Web-based interactive dashboard

📁 How to Run

Locate and double-click the file: CUBE.exe

No installation required – it’s a standalone executable

Ensure the system has write permissions (for exports and PDFs)

Click “Open New Excel File” to load SR data and start working

📌 Data Requirements

Supports: .xlsb, .xlsx, .xls files

Input files must follow a predefined format (consistent column headers, date fields, etc.)

Ensure consistent Product Family naming for grouping features to function properly

🧭 Recommended Workflow

Start Application → Open New Excel File

Apply filters or search SRs in the table view

Generate Excel reports or switch to the dashboard for analytics

Click charts to drill down into SR-level details

Export filtered views or PDFs as needed

🧭 Explanation of Script
CUBE includes specialized Excel report modules (TF1 to TF4), each designed to provide targeted insights into different aspects of Service Request management. These reports utilize specific categorization logic and sheet structures for clarity and actionability.


TF1: Unreceived Meters
Description: This report lists all Service Requests for which the physical meters have not yet arrived at the repair center.
Use Case: It serves as a critical tool for following up on transit delays and reducing initial processing lags, highlighting the first potential bottleneck in the repair process.

Sheets Included:
- Main List
- Categorized Sheets by Ageing
- Branch-Wise View

TF2: Unrepaired Meters
Description: This module identifies SRs where meters have been successfully received by the repair center but are still awaiting or undergoing repair.
Use Case: It provides a visual representation of the active repair queue.

Sheets Included:
- Master List
- By Product Family Group
- Ageing-Based Sheets
- Summary by Status

TF3: Unclosed Repair Lines
Description: This report highlights repaired meters that have not yet been officially closed within the system.
Use Case: It is vital for ensuring compliance with closure procedures.

Sheets Included:
- All Unclosed Lines
- Closure Delay Brackets
- Product Family Breakdown
- Weekly Trend Sheet

TF4: Received Not Repaired (by Category)
Description: This module groups pending meters based on their Product Family Group.
Use Case: It is designed to help identify product lines that are contributing most significantly to the current backlog.

Sheets Included:
- Grouped Category Summary
- Meter-Level Details
- Ageing Based Sheets
- Branch Contribution Sheet

Color Coding Explanation:
- Red cells: Critical status or overdue , (more than 15 days)
- Orange/yellow cells: Under review or moderate ageing less ( more than 7 but less than 15 days)
- Green cells: Low-priority or minimal delays( Less than 7 days )
- Bold Rows/Headers: Visual summary separators

Categorization Logic:
- <15 Days: Routine follow-up
- =15 Days: Zone of concern
- >15 Days: Requires immediate attention


SECTION 6 – Graph Descriptions

The web dashboard integrates several interactive graphs to provide visual insights into the Service Request data:
SRs per Branch: Displays number of SRs raised from each branch. Helps identify high-activity zones.
SR Status Distribution: Pie chart showing percentage of Open vs Closed SRs, helps assess backlog.
SRs Over Time: Line graph of SR creation trend across months, used for seasonal performance tracking.
Component Failure Frequency: Pie chart showing most failed components, guiding repair and inventory priorities.
Product Family Analysis: Bar chart for SR volume per product family group, with drill-down for deeper view.
Stage-wise SR TAT: Stacked bars representing time spent in M1, M2, M3 stages of SR lifecycle.
Month-wise SR TAT: Compares monthly SR closure time under categories like <=21D, >30D, Open.
SR Status by Ageing Category: Visual breakdown of SR ageing buckets across statuses to highlight oldest cases


🧠 Note

This application is intended for CSS Repair Center use only.
Functionality on datasets outside the designed schema may be limited.

📬 Support

For issues, suggestions, or collaboration:
Suvidh Mathur – suvidhmathur25@gmail.com
