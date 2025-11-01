import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import xlsxwriter
import traceback

# Import the centralized clean_column_name function
from data_cleaning_utils import clean_column_name

# Suppress all FutureWarnings from Pandas for cleaner console output
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


def export_received_not_repaired_report(df_original_data, file_path):
    """
    Generates and exports a detailed report of meters that have been received
    (GRN date is available), are not yet repaired (Repair complete date is not available),
    and their SR status is "Open". The report is grouped first by "CON date" category,
    then by SR Number.
    Includes "CON date" (today - GRN date) and categorizes rows by this duration.
    Rows where "Problem Investigation" contains "PHYSICALLY NOT RECEIVED" are highlighted.
    
    Args:
        df_original_data (pd.DataFrame): The full, original DataFrame loaded from Excel.
        file_path (str): The full path where the Excel report should be saved.

    Returns:
        bool: True if the report was generated successfully, False otherwise.
    """
    print("DEBUG: Initiating 'Received But Not Repaired' report generation (tf4_refactored).")

    if df_original_data is None or df_original_data.empty:
        print("No data provided for 'Received But Not Repaired' report.")
        return False

    df_report = df_original_data.copy()

    # Define cleaned column names for robust access
    cleaned_grn_date_col = clean_column_name('GRN date')
    cleaned_repair_complete_date_col = clean_column_name('Repair complete date')
    cleaned_sr_status_col = clean_column_name('SR status')
    cleaned_sr_no_col = clean_column_name('SR no.')
    cleaned_sr_creation_date_col = clean_column_name('SR creation date')
    cleaned_problem_investigation_col = clean_column_name('Problem Investigation')

    # 1. Check for required columns before proceeding
    required_cols = [
        cleaned_grn_date_col,
        cleaned_repair_complete_date_col,
        cleaned_sr_status_col,
        cleaned_sr_no_col,
        cleaned_sr_creation_date_col,
        cleaned_problem_investigation_col
    ]
    missing_cols = [col for col in required_cols if col not in df_report.columns]

    if missing_cols:
        print(f"ERROR: The report cannot be generated. Required columns missing:\n{', '.join(missing_cols)}")
        return False

    # 2. Ensure relevant date columns are datetime objects, coercing errors to NaT
    all_report_date_cols = [
        cleaned_grn_date_col, cleaned_repair_complete_date_col, cleaned_sr_creation_date_col,
        clean_column_name('Repair closure date'), clean_column_name('SR closure date'),
        clean_column_name('Incident Date'), clean_column_name('Sales Shipment Date'),
        clean_column_name('Inter-org challan date from Branch to Repair center'),
        clean_column_name('Inter-org challan date from Repair center to Branch')
    ]
    for col in all_report_date_cols:
        if col in df_report.columns:
            if pd.api.types.is_numeric_dtype(df_report[col]):
                df_report[col] = pd.to_datetime(df_report[col], unit='D', origin='1899-12-30', errors='coerce')
            else:
                df_report[col] = pd.to_datetime(df_report[col], errors='coerce', infer_datetime_format=True)
            
            if pd.api.types.is_datetime64_any_dtype(df_report[col]) and df_report[col].dt.tz is not None:
                df_report[col] = df_report[col].dt.tz_localize(None)

    # Ensure SR no. is string type
    df_report[cleaned_sr_no_col] = df_report[cleaned_sr_no_col].astype(str)

    # 3. Apply the specific filtering logic for this report
    filtered_df = df_report[
        df_report[cleaned_repair_complete_date_col].isna() &          # Repair NOT complete
        df_report[cleaned_grn_date_col].notna() &                     # Meter IS received
        (df_report[cleaned_sr_status_col].astype(str).str.lower() == 'open') # SR is Open
    ].copy()

    if filtered_df.empty:
        print("No meters found that meet the criteria: received, not repaired, and with an open SR status. No report generated.")
        return False

    # 4. Calculate 'CON date' (Days in Repair Center: today - GRN date)
    today = pd.Timestamp.now()
    filtered_df['CON date'] = (today - filtered_df[cleaned_grn_date_col]).dt.days + 1
    filtered_df['CON date'] = filtered_df['CON date'].apply(lambda x: max(0, x) if pd.notna(x) else np.nan)
    filtered_df['CON date'] = filtered_df['CON date'].astype('Int64')

    filtered_df['Days in Repair Center'] = filtered_df['CON date']

    # 5. Add "Con Date Category" column for main grouping
    def categorize_con_date(days):
        if pd.isna(days):
            return "Undefined"
        elif days <= 7:
            return "Received & Unrepaired <= 7 Days"
        elif 7 < days <= 15:
            return "Received & Unrepaired 8-15 Days"
        else: # days > 15
            return "Received & Unrepaired > 15 Days"

    filtered_df['Con Date Category'] = filtered_df['CON date'].apply(categorize_con_date)

    # 6. Define the desired column order for the final Excel report
    canonical_display_order_template = [
        clean_column_name("Branch name"),
        cleaned_sr_no_col,
        clean_column_name("Meter Sr. No."),
        clean_column_name("SR creation date"),
        "CON date",
        clean_column_name("Incident Date"),
        clean_column_name("Item description"),
        cleaned_grn_date_col,
        cleaned_repair_complete_date_col,
        cleaned_sr_status_col,
        'Days in Repair Center',
        clean_column_name("SR Problem type"),
        clean_column_name("Problem Description"),
        clean_column_name("Customer name"),
        clean_column_name("SR summary"),
        clean_column_name("Warranty status"),
        clean_column_name("Product family"),
        clean_column_name("Repair No"),
        clean_column_name("Duration at Repair center"),
        clean_column_name("Current sub-inventory"),
        clean_column_name("Ageing (as on today) from sales shipment"),
        clean_column_name("MTBF"),
        clean_column_name("Defect in Lot(Repair line)"),
        clean_column_name("Symptoms code"),
        clean_column_name("Problem Investigation"),
        clean_column_name("Diagnose code 1-Category"),
        clean_column_name("Diagnose code 1-Description"),
        clean_column_name("Diagnose code 2-Category"),
        clean_column_name("Diagnose code 2-Description"),
        clean_column_name("Diagnose code 3-Category"),
        clean_column_name("Diagnose code 3-Description"),
        clean_column_name("Service code"),
        clean_column_name("Repair closure date"),
        clean_column_name("Customer Demand class"),
        clean_column_name("End Customer name"),
        clean_column_name("End customer Demand class"),
        clean_column_name("Inter-org challan No. from Branch to Repair center"),
        clean_column_name("Inter-org challan date from Branch to Repair center"),
        clean_column_name("Inter-org challan No. from Repair center to Branch"),
        clean_column_name("Inter-org challan date from Repair center to Branch"),
        clean_column_name("WO status (Yes / No)"),
        clean_column_name("Sales Order No."),
        clean_column_name("Sales Shipment Date"),
        clean_column_name("Unit"),
        clean_column_name("Courier Name"),
        clean_column_name("Courier Mode"),
        clean_column_name("Courier Number"),
        clean_column_name("SR closure date"),
    ]

    final_export_columns = [col for col in canonical_display_order_template if col in filtered_df.columns]

    if 'Con Date Category' in final_export_columns:
        final_export_columns.remove('Con Date Category')
    
    filtered_df = filtered_df.reindex(columns=final_export_columns + ['Con Date Category']) 

    print(f"DEBUG: Final columns for export (including internal 'Con Date Category'):\n{filtered_df.columns.tolist()}")

    try:
        writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
        workbook = writer.book
        worksheet = workbook.add_worksheet('Received & Unrepaired')

        # Define general cell format properties (dictionaries)
        date_format_props_base = {'num_format': 'dd-mmm-yyyy', 'align': 'left', 'valign': 'top'}
        integer_format_props_base = {'num_format': '#,##0', 'align': 'right', 'valign': 'top'}
        text_format_props_base = {'text_wrap': False, 'align': 'left', 'valign': 'top'}

        # Define visual style properties (dictionaries)
        header_format_props = {'bold': True, 'text_wrap': False, 'valign': 'top', 'border': 1, 'align': 'center', 'bg_color': '#D9E1F2'}
        main_category_label_format_props = {'bold': True, 'font_size': 14, 'align': 'left', 'valign': 'vcenter', 'bg_color': '#A6D9F7', 'font_color': '#333333', 'border': 1}
        unique_sr_count_format_props = {'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter', 'bg_color': '#D9E1F2', 'font_color': '#444444', 'border': 1}
        sr_group_label_format_props = {'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter', 'bg_color': '#ADD8E6', 'font_color': '#000000'}

        # Row-level color base properties (dictionaries)
        green_row_base_props = {'bg_color': '#C6EFCE', 'font_color': '#006100'} # Green for <=7D
        yellow_row_base_props = {'bg_color': '#FFEB9C', 'font_color': '#9C6500'} # Yellow for >7D and <=15D
        red_row_base_props = {'bg_color': '#FFC7CE', 'font_color': '#9C0006'}   # Red for >15D
        default_row_base_props = {'bg_color': '#FFFFFF', 'font_color': '#000000'} # White for undefined/no special category
        problem_highlight_props = {'bg_color': '#FFD700', 'font_color': '#8B4513'} # Gold for "PHYSICALLY NOT RECEIVED"

        # Create actual format objects from dictionaries
        header_format = workbook.add_format(header_format_props)
        main_category_label_format = workbook.add_format(main_category_label_format_props)
        unique_sr_count_format = workbook.add_format(unique_sr_count_format_props)
        sr_group_label_format = workbook.add_format(sr_group_label_format_props)

        # Base formats for set_column (no background, just num_format/text_wrap etc.)
        date_column_format = workbook.add_format(date_format_props_base)
        integer_column_format = workbook.add_format(integer_format_props_base)
        text_column_format = workbook.add_format(text_format_props_base)


        current_row = 0

        # Write main column headers at the very top of the sheet
        for col_num, col_name in enumerate(final_export_columns):
            worksheet.write(current_row, col_num, col_name, header_format)
        current_row += 1

        # Set static column widths for the entire sheet using base formats
        for i, col_name in enumerate(final_export_columns):
            current_col_name_cleaned = clean_column_name(col_name)
            if 'date' in current_col_name_cleaned.lower():
                worksheet.set_column(i, i, 15, date_column_format)
            elif current_col_name_cleaned in ["CON date", "Days in Repair Center"]:
                worksheet.set_column(i, i, 18, integer_column_format)
            elif 'description' in current_col_name_cleaned.lower() or 'summary' in current_col_name_cleaned.lower() or 'problem' in current_col_name_cleaned.lower() or 'investigation' in current_col_name_cleaned.lower():
                worksheet.set_column(i, i, 40, text_column_format)
            elif 'sr no' in current_col_name_cleaned.lower() or 'meter sr. no.' in current_col_name_cleaned.lower():
                worksheet.set_column(i, i, 15, text_column_format)
            else: # Default for other columns, attempt numeric first
                if col_name in filtered_df.columns and pd.api.types.is_numeric_dtype(filtered_df[col_name]):
                    worksheet.set_column(i, i, 12, integer_column_format)
                else:
                    worksheet.set_column(i, i, 15, text_column_format)


        # Define the order of main categories for display
        main_category_order = [
            "Received & Unrepaired <= 7 Days",
            "Received & Unrepaired 8-15 Days",
            "Received & Unrepaired > 15 Days",
            "Undefined" # Always keep last for any edge cases
        ]

        # Iterate through each main "Con Date Category"
        for category_label in main_category_order:
            # Get all rows that belong to the current category label
            category_df = filtered_df[filtered_df['Con Date Category'] == category_label].copy()

            if not category_df.empty:
                # Add extra blank row for visual separation between main categories
                if current_row > (len(final_export_columns)):
                    current_row += 1

                # Write the main category label (merged across all columns)
                worksheet.merge_range(current_row, 0, current_row, len(final_export_columns) - 1,
                                      category_label.upper(), main_category_label_format)
                current_row += 1

                # Calculate unique SR count for the current category
                unique_srs_in_category_count = category_df[cleaned_sr_no_col].nunique()

                # Write the unique SR count text
                worksheet.merge_range(current_row, 0, current_row, len(final_export_columns) - 1,
                                      f"Count of Unique SRs: {unique_srs_in_category_count}", unique_sr_count_format)
                current_row += 1

                # Sort by SR Number within this category for consistent sub-grouping
                category_df.sort_values(by=cleaned_sr_no_col, inplace=True)

                # Now, group by SR Number within this category
                for sr_num, sr_group_df in category_df.groupby(cleaned_sr_no_col):
                    if not sr_group_df.empty:
                        # Add a blank row for visual separation between SR groups within a category
                        if current_row > (worksheet.dim_rowmax + 1):
                             current_row += 1

                        # Write the SR group label (e.g., "SR: 12345")
                        worksheet.merge_range(current_row, 0, current_row, len(final_export_columns) - 1,
                                              f"SR: {sr_num}", sr_group_label_format)
                        current_row += 1

                        # Write data rows for the current SR group with conditional formatting
                        for _, row_data in sr_group_df.iterrows():
                            con_date_days = row_data.get('CON date')

                            actual_problem_investigation_val = row_data.get(cleaned_problem_investigation_col)
                            problem_investigation_text_for_check = ''
                            if pd.notna(actual_problem_investigation_val):
                                problem_investigation_text_for_check = str(actual_problem_investigation_val).lower()

                            # Determine the base row style properties: start with age-based, then override if problem investigation matches
                            row_base_format_options = dict(default_row_base_props)
                            if pd.notna(con_date_days):
                                if con_date_days <= 7:
                                    row_base_format_options.update(green_row_base_props)
                                elif 7 < con_date_days <= 15:
                                    row_base_format_options.update(yellow_row_base_props)
                                else: # con_date_days > 15
                                    row_base_format_options.update(red_row_base_props)

                            # Check for "PHYSICALLY NOT RECEIVED" and override color
                            if "physically not received" in problem_investigation_text_for_check:
                                row_base_format_options.update(problem_highlight_props)

                            # Create a single row format object that will be used for all cells in this row
                            current_row_format_obj = workbook.add_format(row_base_format_options)
                            
                            for col_num, col_name in enumerate(final_export_columns):
                                value = row_data.get(col_name)

                                # Create a temporary cell format by combining row's properties with specific data type properties
                                cell_format_props = dict(row_base_format_options)
                                
                                current_col_name_cleaned = clean_column_name(col_name)

                                if current_col_name_cleaned in [clean_column_name(d) for d in ["SR creation date", "Incident Date", "GRN date", "Repair complete date", "SR closure date", "Inter-org challan date from Branch to Repair center", "Inter-org challan date from Repair center to Branch", "Sales Shipment Date", "Repair closure date"]]:
                                    cell_format_props.update(date_format_props_base)
                                elif current_col_name_cleaned in ["CON date", "Days in Repair Center"] or ('ageing' in current_col_name_cleaned.lower()):
                                    cell_format_props.update(integer_format_props_base)
                                elif pd.api.types.is_numeric_dtype(type(value)):
                                    cell_format_props.update(integer_format_props_base)
                                else:
                                    cell_format_props.update(text_format_props_base)

                                final_cell_format_obj = workbook.add_format(cell_format_props)

                                # Write the cell value using the determined format
                                if pd.isna(value):
                                    worksheet.write_blank(current_row, col_num, '', final_cell_format_obj)
                                elif isinstance(value, (datetime, pd.Timestamp)):
                                    worksheet.write_datetime(current_row, col_num, value, final_cell_format_obj)
                                elif pd.api.types.is_numeric_dtype(type(value)):
                                    if float(value) == int(value):
                                        worksheet.write_number(current_row, col_num, int(value), final_cell_format_obj)
                                    else:
                                        worksheet.write_number(current_row, col_num, value, final_cell_format_obj)
                                else:
                                    worksheet.write_string(current_row, col_num, str(value), final_cell_format_obj)
                            current_row += 1

        workbook.close()
        print(f"Report 'Received But Not Repaired Meters' successfully saved to: {file_path}")
        return True

    except Exception as e:
        traceback.print_exc()
        print(f"An unexpected error occurred during report generation:\n{e}")
        return False

# For direct testing of the module:
if __name__ == '__main__':
    dummy_data = {
        "Branch name": ["B1", "B2", "B1", "B3", "B2", "B1"],
        "SR no.": ["SR001", "SR002", "SR003", "SR004", "SR005", "SR006"],
        "SR creation date": [datetime(2025, 1, 1), datetime(2025, 1, 5), datetime(2025, 1, 10), datetime(2025, 2, 1), datetime(2025, 2, 5), datetime(2025, 1, 20)],
        "GRN date": [datetime(2025, 1, 3), datetime(2025, 1, 7), datetime(2025, 1, 11), datetime(2025, 2, 2), datetime(2025, 2, 6), pd.NaT],
        "Repair complete date": [pd.NaT, pd.NaT, datetime(2025, 1, 12), pd.NaT, datetime(2025, 2, 8), pd.NaT],
        "SR status": ["Open", "Open", "Open", "Open", "Closed", "Open"],
        "Problem Investigation": ["Normal", "PHYSICALLY NOT RECEIVED", "Repaired fine", "Normal", "Closed out", "Issue with GRN"],
        "Meter Sr. No.": ["M001", "M002", "M003", "M004", "M005", "M006"],
        "Item description": ["Item A", "Item B", "Item C", "Item D", "Item E", "Item F"],
        "Product family": ["Saral 100", "Prodigy", "Elite 440", "Apex", "Freedoms", "Saral 300"],
        "Customer name": ["Cust1", "Cust2", "Cust3", "Cust4", "Cust5", "Cust6"],
        "SR summary": ["Summary 1", "Summary 2", "Summary 3", "Summary 4", "Summary 5", "Summary 6"],
        "Warranty status": ["In Warranty", "Out of Warranty", "In Warranty", "In Warranty", "Out of Warranty", "In Warranty"],
        "Incident Date": [datetime(2025,1,1), datetime(2025,1,5), datetime(2025,1,10), datetime(2025,2,1), datetime(2025,2,5), datetime(2025,1,20)],
        "Repair No": [101, 102, 103, 104, 105, 106],
        "Duration at Repair center": [5, 10, 8, 3, 7, 0],
        "Current sub-inventory": ["SubA", "SubB", "SubA", "SubC", "SubB", "SubA"],
        "Ageing (as on today) from sales shipment": [120, 115, 100, 90, 80, 70],
        "MTBF": [365, 300, 400, 350, 280, 320],
        "Defect in Lot(Repair line)": ["No", "Yes", "No", "No", "Yes", "No"],
        "Symptoms code": ["S01", "S02", "S03", "S04", "S05", "S06"],
        "Diagnose code 1-Category": ["CatA", "CatB", "CatC", "CatA", "CatB", "CatC"],
        "Diagnose code 1-Description": ["D1", "D2", "D3", "D4", "D5", "D6"],
        "Diagnose code 2-Category": ["CatX", pd.NaT, "CatY", pd.NaT, "CatZ", "CatX"],
        "Diagnose code 2-Description": ["DX1", "", "DY1", "", "DZ1", "DX1"],
        "Diagnose code 3-Category": [pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT],
        "Diagnose code 3-Description": [pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT],
        "Service code": ["SVC1", "SVC2", "SVC3", "SVC4", "SVC5", "SVC6"],
        "Repair closure date": [pd.NaT, pd.NaT, datetime(2025, 1, 15), pd.NaT, datetime(2025, 2, 10), pd.NaT],
        "SR closure date": [datetime(2025, 1, 10), pd.NaT, datetime(2025, 1, 15), pd.NaT, datetime(2025, 2, 10), pd.NaT],
        "Inter-org challan No. from Branch to Repair center": ["ICH1", "ICH2", "ICH3", "ICH4", "ICH5", "ICH6"],
        "Inter-org challan date from Branch to Repair center": [datetime(2025,1,2), datetime(2025,1,6), datetime(2025,1,10), datetime(2025,2,2), datetime(2025,2,6), datetime(2025,1,21)],
        "Inter-org challan No. from Repair center to Branch": ["IRCH1", "IRCH2", "IRCH3", "IRCH4", "IRCH5", "IRCH6"],
        "Inter-org challan date from Repair center to Branch": [datetime(2025,1,5), datetime(2025,1,15), datetime(2025,1,13), datetime(2025,2,5), datetime(2025,2,9), datetime(2025,1,23)],
        "WO status (Yes / No)": ["Yes", "No", "Yes", "No", "Yes", "No"],
        "Sales Order No.": ["SO1", "SO2", "SO3", "SO4", "SO5", "SO6"],
        "Unit": ["Unit1", "Unit2", "Unit3", "Unit4", "Unit5", "Unit6"],
        "Courier Name": ["C1", "C2", "C3", "C4", "C5", "C6"],
        "Courier Mode": ["Mode1", "Mode2", "Mode3", "Mode4", "Mode5", "Mode6"],
        "Courier Number": ["CN1", "CN2", "CN3", "CN4", "CN5", "CN6"]
    }
    dummy_df = pd.DataFrame(dummy_data)
    dummy_df.columns = [clean_column_name(col) for col in dummy_df.columns]

    print("Running tf4_received_not_repaired_report.py directly for testing...")
    output_path = "test_received_not_repaired_report.xlsx"
    success = export_received_not_repaired_report(dummy_df, output_path)
    if success:
        print(f"Test report saved to {output_path}")
    else:
        print("Test report generation failed.")