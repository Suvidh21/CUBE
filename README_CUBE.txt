
# CUBE – Service Request Data Management & Visualization Tool

**Developed by:** Suvidh Mathur (intern) 
**Organization:** Secure Meters Limited  
**Deployment Date:** July 15, 2025  
**Contact:** suvidhmathur25@gmail.com  

---

## 🧩 Overview

**CUBE** is a robust desktop-based application designed to **streamline data management, prioritization, and analysis** of Service Request (SR) data for the CSS Repair Center. The tool automates Turnaround Time (TAT) calculations, enables precise filtering and editing of records, and generates targeted Excel reports to support operational decision-making and improve service efficiency.

---

## 🚀 Key Features

### 🔹 Tkinter Desktop Interface
- Import raw Excel files (.xlsb, .xlsx, .xls)
- Apply filters: SR Type, SR Status, Product Family, Custom Date Range
- Edit table data directly with validation
- Export filtered data to Excel
- One-click report generation for:
  - Unreceived Meters
  - Unrepaired Meters
  - Unclosed Repair Lines
  - Received Not Repaired by Category

### 🔹 Dash Web Dashboard (Auto-Refreshing Every 15 sec)
- Interactive graphs for SR trends, TAT performance, component failures, backlog ageing, etc.
- Filters for SR type, status, ageing, product group, and date range
- Drill-down: Click any graph to auto-filter corresponding SRs in the desktop app
- Export dashboard view to PDF

---

## ⚙️ Technology Stack

- Python  
- Tkinter (Desktop UI)  
- Pandas, Openpyxl (Excel handling)  
- Plotly Dash (Web-based visualization)  

---

## 📁 How to Run

1. Locate and double-click the file: `CUBE.exe`
2. No installation is required – this is a standalone executable.
3. Ensure the system has basic write permissions (for saving exports and PDFs).
4. The application opens with a simple GUI. Use the `Open New Excel File` button to start.

---

## 📌 Data Requirements

- Only `.xlsb`, `.xlsx`, or `.xls` files are supported.
- Input Excel files must follow the predefined format (column headers, date fields, etc.)
- Product Family values should be consistent for grouping features to work properly.

---

## 📄 Recommended Use Flow

1. **Start Application** → `Open New Excel File`
2. Apply filters or search for SRs using the table interface
3. Generate Excel reports or analyze through the auto-refreshing dashboard
4. Click on dashboard visuals to deep-dive into SR-level details
5. Export PDFs or filtered views as needed

---

## 🧠 Note

This application is designed for use by the CSS Repair Center only. It may not function correctly on datasets outside its intended schema or in unrelated departments.

---

## 📬 Support

For any queries or suggestions, contact:  
**Suvidh Mathur** – suvidhmathur25@gmail.com
