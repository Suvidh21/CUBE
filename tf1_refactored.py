# tf1_refactored.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import xlsxwriter
import traceback
import time

# Import the centralized clean_column_name function from the new utility file
from data_cleaning_utils import clean_column_name

# Helper to write a single DataFrame to a specific worksheet with custom date formatting,
# column widths, and header/data writing logic.
def _write_single_df_to_worksheet_tf1(df_to_write, worksheet, workbook, start_row=0, date_cols_to_format=None):
    """
    Helper to write a single DataFrame to a specific worksheet with custom date formatting,
    column widths, and header/data writing logic.
    `start_row`: The row number to begin writing data (0-indexed).
    `date_cols_to_format`: List of cleaned column names expected to be dates.
    """
    if date_cols_to_format is None:
        date_cols_to_format = []

    date_format = workbook.add_format({'num_format': 'dd-mmm-yyyy'})
    integer_format = workbook.add_format({'num_format': '#,##0'})
    text_format = workbook.add_format({'text_wrap': False})
    header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'border': 1, 'align': 'center'})

    for col_num, value in enumerate(df_to_write.columns.values):
        worksheet.write(start_row, col_num, value, header_format)

    for i, col_name in enumerate(df_to_write.columns):
        current_col_name_cleaned = clean_column_name(col_name)

        if current_col_name_cleaned in date_cols_to_format:
            worksheet.set_column(i, i, 15, date_format)
        elif 'description' in current_col_name_cleaned.lower() or \
                'summary' in current_col_name_cleaned.lower() or \
                'problem' in current_col_name_cleaned.lower() or \
                'investigation' in current_col_name_cleaned.lower():
            worksheet.set_column(i, i, 40, text_format)
        elif current_col_name_cleaned == 'Days Passed' or current_col_name_cleaned == 'con date' or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
            worksheet.set_column(i, i, 15, integer_format)
        elif pd.api.types.is_numeric_dtype(df_to_write[col_name]):
            worksheet.set_column(i, i, 12, integer_format)
        else:
            worksheet.set_column(i, i, 15)

    for row_num, (index, row_data) in enumerate(df_to_write.iterrows()):
        excel_row_num = row_num + 1 + start_row

        for col_num, col_name in enumerate(df_to_write.columns):
            value = row_data[col_name]

            final_cell_format = text_format
            current_col_name_cleaned = clean_column_name(col_name)
            if current_col_name_cleaned in date_cols_to_format:
                final_cell_format = date_format
            elif current_col_name_cleaned in ['Days Passed', 'con date'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
                final_cell_format = integer_format
            elif pd.api.types.is_numeric_dtype(type(value)):
                if float(value) == int(value):
                    worksheet.write_number(excel_row_num, col_num, int(value))
                else:
                    worksheet.write_number(excel_row_num, col_num, value)
            else:
                worksheet.write_string(excel_row_num, col_num, str(value))


def _write_categorized_sr_counts_sheet_tf1(df_original, workbook, worksheet_name, date_cols_to_format=None):
    """
    Prepares and writes the 'SR counts' sheet with unique, unreceived, and open SRs,
    categorizing them by 'Days Passed'. This sheet will have section headers and row coloring.
    `date_cols_to_format`: List of cleaned column names expected to be dates.
    This function expects df_original to have already cleaned column names.
    """
    if df_original is None or df_original.empty:
        print(f"No original data to write to '{worksheet_name}' sheet.")
        return False # Indicate failure if no data

    worksheet = workbook.add_worksheet(worksheet_name)

    header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'border': 1, 'align': 'center'})
    date_format = workbook.add_format({'num_format': 'dd-mmm-yyyy'})
    integer_format = workbook.add_format({'num_format': '#,##0'})
    text_format = workbook.add_format({'text_wrap': False})

    green_row_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
    yellow_row_format = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})
    red_row_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
    purple_row_format = workbook.add_format({'bg_color': '#E2CFF4', 'font_color': '#5B00B7'})
    invalid_sr_row_format = workbook.add_format({'bg_color': '#D3D3D3', 'font_color': '#333333'}) # Light Gray

    category_label_format = workbook.add_format({
        'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter',
        'bg_color': '#ADD8E6', 'font_color': '#000000'
    })
    total_count_format = workbook.add_format({'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter'})

    df_working = df_original.copy() # Renamed to df_working for clarity on its changing state

    cleaned_sr_creation_date_col = clean_column_name('SR creation date')
    cleaned_grn_date_col = clean_column_name('GRN date')
    cleaned_sr_closure_date_col = clean_column_name('SR closure date')
    cleaned_sr_status_col = clean_column_name('SR status')
    cleaned_sr_no_col = clean_column_name('SR no.')

    # --- DEBUG START ---
    tracking_sr_no = "546691" # Keep this set to the SR you're investigating
    print(f"DEBUGGING SR Counts Sheet for SR: {tracking_sr_no}")
    initial_presence = tracking_sr_no in df_working[cleaned_sr_no_col].values if cleaned_sr_no_col in df_working.columns else False
    print(f"DEBUG: Initial presence of {tracking_sr_no}: {initial_presence}")
    # --- DEBUG END ---

    # Ensure date columns are proper datetime objects
    if cleaned_sr_creation_date_col in df_working.columns:
        df_working[cleaned_sr_creation_date_col] = pd.to_datetime(df_working[cleaned_sr_creation_date_col], errors='coerce')
    if cleaned_grn_date_col in df_working.columns:
        # Crucial for GRN date: ensure it's coerced to NaT if blank/empty string
        df_working[cleaned_grn_date_col] = df_working[cleaned_grn_date_col].replace('', np.nan).replace(' ', np.nan)
        df_working[cleaned_grn_date_col] = pd.to_datetime(df_working[cleaned_grn_date_col], errors='coerce')
    if cleaned_sr_closure_date_col in df_working.columns:
        df_working[cleaned_sr_closure_date_col] = pd.to_datetime(df_working[cleaned_sr_closure_date_col], errors='coerce')

    # --- DEBUG START ---
    presence_after_initial_date_conversion_tf1 = tracking_sr_no in df_working[cleaned_sr_no_col].values if cleaned_sr_no_col in df_working.columns else False
    print(f"DEBUG: Presence of {tracking_sr_no} after initial date conversion (tf1_refactored): {presence_after_initial_date_conversion_tf1}")
    if presence_after_initial_date_conversion_tf1:
        sr_data_after_date_conv_tf1 = df_working[df_working[cleaned_sr_no_col] == tracking_sr_no]
        if not sr_data_after_date_conv_tf1.empty:
            print(f"  SR {tracking_sr_no} GRN Date(s) after initial date conversion (tf1_refactored):")
            print(sr_data_after_date_conv_tf1[[cleaned_sr_no_col, cleaned_grn_date_col]].to_string(index=False))
            print(f"  SR {tracking_sr_no} Status(es):")
            print(sr_data_after_date_conv_tf1[[cleaned_sr_no_col, cleaned_sr_status_col]].to_string(index=False))
    # --- DEBUG END ---


    # Step 1: Filter out 'Closed' SRs first (SR counts is for open SRs)
    if cleaned_sr_status_col in df_working.columns:
        df_open_srs = df_working[df_working[cleaned_sr_status_col].astype(str).str.lower() != 'closed'].copy()
    else:
        print(f"Warning: '{cleaned_sr_status_col}' not found for filtering out 'Closed' status in '{worksheet_name}' sheet.")
        df_open_srs = pd.DataFrame(columns=df_working.columns) # Empty if column missing

    # --- DEBUG START ---
    presence_after_status_filter = tracking_sr_no in df_open_srs[cleaned_sr_no_col].values if cleaned_sr_no_col in df_open_srs.columns else False
    print(f"DEBUG: Presence of {tracking_sr_no} after SR status filter: {presence_after_status_filter}")
    if presence_after_status_filter:
        print(f"  SR {tracking_sr_no} GRN Date(s) after status filter:")
        print(df_open_srs[df_open_srs[cleaned_sr_no_col] == tracking_sr_no][[cleaned_sr_no_col, cleaned_grn_date_col]].to_string(index=False))
    # --- DEBUG END ---

    # Step 2: From the OPEN SRs, find all lines that are UNRECEIVED (GRN date is blank)
    if cleaned_grn_date_col in df_open_srs.columns:
        df_unreceived_lines = df_open_srs[df_open_srs[cleaned_grn_date_col].isna()].copy()
    else:
        print(f"Warning: '{cleaned_grn_date_col}' not found for filtering by unreceived items in '{worksheet_name}' sheet.")
        df_unreceived_lines = pd.DataFrame(columns=df_open_srs.columns) # Empty if column missing

    # --- DEBUG START ---
    presence_after_unreceived_lines_filter = tracking_sr_no in df_unreceived_lines[cleaned_sr_no_col].values if cleaned_sr_no_col in df_unreceived_lines.columns else False
    print(f"DEBUG: Presence of {tracking_sr_no} after filtering for UNRECEIVED lines: {presence_after_unreceived_lines_filter}")
    if presence_after_unreceived_lines_filter:
        print(f"  SR {tracking_sr_no} GRN Date(s) in unreceived_lines DF:")
        print(df_unreceived_lines[df_unreceived_lines[cleaned_sr_no_col] == tracking_sr_no][[cleaned_sr_no_col, cleaned_grn_date_col]].to_string(index=False))
    # --- DEBUG END ---

    # Step 3: Get the unique SR Numbers from these UNRECEIVED lines.
    # These are the SRs that should be counted on the SR Counts sheet.
    if cleaned_sr_no_col in df_unreceived_lines.columns:
        unique_unreceived_sr_numbers = df_unreceived_lines[cleaned_sr_no_col].unique()
        
        # Now, filter the original df_open_srs to only include these unique SRs.
        # This df_to_process will be the source for SR counts.
        df_to_process = df_open_srs[df_open_srs[cleaned_sr_no_col].isin(unique_unreceived_sr_numbers)].copy()
        
        # To get a single representative row for each SR for the SR Counts sheet,
        # we sort to prioritize lines with blank GRN dates, then drop duplicates.
        if cleaned_grn_date_col in df_to_process.columns:
            # Create a helper column: True if GRN date is NaT (unreceived), False if it has a date
            df_to_process['__grn_is_na_priority'] = df_to_process[cleaned_grn_date_col].isna()
            # Sort by SR number, then by this priority column.
            # `ascending=False` for '__grn_is_na_priority' means True (NaT) comes before False (date).
            # So, `keep='first'` will select the unreceived line if one exists for that SR.
            df_to_process = df_to_process.sort_values(
                by=[cleaned_sr_no_col, '__grn_is_na_priority'], ascending=[True, False]
            ).drop_duplicates(subset=[cleaned_sr_no_col], keep='first').drop(columns=['__grn_is_na_priority']).copy()

        else: # Fallback if GRN date column is somehow missing
            df_to_process = df_to_process.drop_duplicates(subset=[cleaned_sr_no_col], keep='first').copy()
            
    else: # Fallback if SR no. column is missing or no unreceived lines were found
        df_to_process = pd.DataFrame(columns=df_original.columns) # No SRs to process for counts


    # --- DEBUG START ---
    presence_after_final_dedup_tf1 = tracking_sr_no in df_to_process[cleaned_sr_no_col].values if cleaned_sr_no_col in df_to_process.columns else False
    print(f"DEBUG: Presence of {tracking_sr_no} after final unique SR selection for counts sheet: {presence_after_final_dedup_tf1}")
    if presence_after_final_dedup_tf1:
        sr_data_final_tf1 = df_to_process[df_to_process[cleaned_sr_no_col] == tracking_sr_no].iloc[0]
        print(f"  Final GRN Date for {tracking_sr_no} in SR counts DF: '{sr_data_final_tf1.get(cleaned_grn_date_col)}'")
        print(f"  Final SR Creation Date for {tracking_sr_no} in SR counts DF: '{sr_data_final_tf1.get(cleaned_sr_creation_date_col)}'")
        print(f"  Final SR Status for {tracking_sr_no} in SR counts DF: '{sr_data_final_tf1.get(cleaned_sr_status_col)}'")
    # --- DEBUG END ---

    if df_to_process.empty:
        print(f"No data remaining after final filtering for '{worksheet_name}' sheet.")
        worksheet.write(0, 0, "No data available for SR Counts sheet based on current filters.", total_count_format)
        return False # Indicate failure if no data

    today = pd.Timestamp.now()

    # Calculate Days Passed (con date) - This logic remains the same
    df_to_process['con date'] = np.where(
        df_to_process[cleaned_sr_creation_date_col].notna(),
        (today - df_to_process[cleaned_sr_creation_date_col]).dt.days + 1,
        np.nan # If SR creation date is NaT, set to NaN
    )
    df_to_process['con date'] = df_to_process['con date'].apply(lambda x: max(0, x) if pd.notna(x) else np.nan)
    df_to_process['Days Passed'] = df_to_process['con date']

    def categorize_days_passed(days):
        if pd.isna(days):
            return "Invalid SR"
        elif days <= 7:
            return "less than or equal to 7 days"
        elif 7 < days <= 15:
            return "more than 7 but less than or equal to 15 days"
        else:
            return "more than 15 days"

    df_to_process['Days Passed Category'] = df_to_process['Days Passed'].apply(categorize_days_passed)

    # --- DEBUG START ---
    if presence_after_final_dedup_tf1: # Only if it made it this far
        sr_data_after_days_calc = df_to_process[df_to_process[cleaned_sr_no_col] == tracking_sr_no].iloc[0]
        print(f"DEBUG: Data for {tracking_sr_no} after 'Days Passed' calculation:")
        print(f"  con date: {sr_data_after_days_calc.get('con date')}")
        print(f"  Days Passed: {sr_data_after_days_calc.get('Days Passed')}")
        print(f"  Days Passed Category: {sr_data_after_days_calc.get('Days Passed Category')}")
    # --- DEBUG END ---

    canonical_display_order_for_sr_counts = [
        clean_column_name("Branch name"),
        clean_column_name("SR no."),
        clean_column_name("SR creation date"),
        "con date",
        clean_column_name("Incident Date"),
        clean_column_name("SR status"),
        clean_column_name("SR Problem type"),
        clean_column_name("Repair No"),
        clean_column_name("Meter Sr. No."),
        clean_column_name("Duration at Repair center"),
        clean_column_name("Item description"),
        clean_column_name("Product family"),
        clean_column_name("GRN date"),
        clean_column_name("Inter-org challan date from Branch to Repair center"),
        clean_column_name("Inter-org challan date from Repair center to Branch"),
        clean_column_name("Sales Shipment Date"),
        "Ageing (Calculated)", # This column is added in ex1.py
        clean_column_name("Defect in Lot(Repair line)"),
        clean_column_name("Problem Description"), clean_column_name("Problem Investigation"),
        clean_column_name("Diagnose code 1-Category"), clean_column_name("Diagnose code 1-Description"),
        clean_column_name("Diagnose code 2-Category"), clean_column_name("Diagnose code 2-Description"),
        clean_column_name("Diagnose code 3-Category"), clean_column_name("Diagnose code 3-Description"),
        clean_column_name("Service code"),
        clean_column_name("Repair complete date"),
        clean_column_name("Repair closure date"),
        clean_column_name("Customer name"), clean_column_name("SR summary"),
        clean_column_name("Warranty status"), clean_column_name("SR closure date"),
        'Days Passed', 'Days Passed Category'
    ]
    final_sr_counts_columns = [col for col in canonical_display_order_for_sr_counts if col in df_to_process.columns]
    df_to_process = df_to_process.reindex(columns=final_sr_counts_columns)


    df_purple_category_sr_counts = pd.DataFrame(columns=df_to_process.columns)
    has_sr_closure_date_col = cleaned_sr_closure_date_col in df_original.columns # Check original for purple lines
    if has_sr_closure_date_col and cleaned_grn_date_col in df_original.columns:
        # Filter for the 'purple' category: unreceived (GRN is NA) BUT SR is closed (SR Closure Date is NOT NA)
        # Use df_original here to get all such lines, then deduplicate by SR no. for the count sheet
        purple_mask = (df_original[cleaned_grn_date_col].isna() &
                       df_original[cleaned_sr_closure_date_col].notna() &
                       df_original[cleaned_sr_status_col].astype(str).str.lower() == 'closed')
        df_purple_category_sr_counts = df_original[purple_mask].drop_duplicates(subset=[cleaned_sr_no_col]).copy()

    # --- DEBUG START ---
    presence_in_purple = tracking_sr_no in df_purple_category_sr_counts[cleaned_sr_no_col].values if cleaned_sr_no_col in df_purple_category_sr_counts.columns else False
    print(f"DEBUG: Presence of {tracking_sr_no} in purple category: {presence_in_purple}")
    # --- DEBUG END ---

    df_cat1_le_7_sr_counts = df_to_process[df_to_process['Days Passed Category'] == "less than or equal to 7 days"].sort_values(by='Days Passed', ascending=True)
    df_cat2_gt7_le15_sr_counts = df_to_process[df_to_process['Days Passed Category'] == "more than 7 but less than or equal to 15 days"].sort_values(by='Days Passed', ascending=True)
    df_cat3_gt_15_sr_counts = df_to_process[df_to_process['Days Passed Category'] == "more than 15 days"].sort_values(by='Days Passed', ascending=True)
    df_invalid_sr_counts = df_to_process[df_to_process['Days Passed Category'] == "Invalid SR"].copy()

    # --- DEBUG START ---
    presence_in_cat1 = tracking_sr_no in df_cat1_le_7_sr_counts[cleaned_sr_no_col].values if cleaned_sr_no_col in df_cat1_le_7_sr_counts.columns else False
    presence_in_cat2 = tracking_sr_no in df_cat2_gt7_le15_sr_counts[cleaned_sr_no_col].values if cleaned_sr_no_col in df_cat2_gt7_le15_sr_counts.columns else False
    presence_in_cat3 = tracking_sr_no in df_cat3_gt_15_sr_counts[cleaned_sr_no_col].values if cleaned_sr_no_col in df_cat3_gt_15_sr_counts.columns else False
    presence_in_invalid = tracking_sr_no in df_invalid_sr_counts[cleaned_sr_no_col].values if cleaned_sr_no_col in df_invalid_sr_counts.columns else False
    print(f"DEBUG: Presence of {tracking_sr_no} in category 'le 7 days': {presence_in_cat1}")
    print(f"DEBUG: Presence of {tracking_sr_no} in category 'gt 7 le 15 days': {presence_in_cat2}")
    print(f"DEBUG: Presence of {tracking_sr_no} in category 'gt 15 days': {presence_in_cat3}")
    print(f"DEBUG: Presence of {tracking_sr_no} in category 'Invalid SR': {presence_in_invalid}")
    # --- DEBUG END ---

    current_row = 0

    # Total count now reflects the unique SRs that are open AND have at least one unreceived meter
    # plus the unique SRs that are closed AND unreceived.
    total_sr_count = len(df_to_process) + len(df_purple_category_sr_counts)
    worksheet.write(current_row, 0, f"Total Unreceived SRs : {total_sr_count}", total_count_format)
    current_row += 2

    for col_num, col_name in enumerate(final_sr_counts_columns):
        worksheet.write(current_row, col_num, col_name, header_format)
    current_row += 1

    for i, col_name in enumerate(final_sr_counts_columns):
        current_col_name_cleaned = clean_column_name(col_name)
        if current_col_name_cleaned in date_cols_to_format:
            worksheet.set_column(i, i, 15, date_format)
        elif 'description' in current_col_name_cleaned.lower() or \
                'summary' in current_col_name_cleaned.lower() or \
                'problem' in current_col_name_cleaned.lower() or \
                'investigation' in current_col_name_cleaned.lower():
            worksheet.set_column(i, i, 40, text_format)
        elif current_col_name_cleaned in ['Days Passed', 'con date'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
            worksheet.set_column(i, i, 15, integer_format)
        elif col_name in df_to_process.columns and pd.api.types.is_numeric_dtype(df_to_process[col_name]):
            worksheet.set_column(i, i, 12, integer_format)
        else:
            worksheet.set_column(i, i, 15)


    def write_category_block_with_label_internal(df_category, label_text, row_color_format, current_row_ref, columns_order):
        if not df_category.empty:
            if current_row_ref[0] > 1:
                current_row_ref[0] += 1

            worksheet.merge_range(current_row_ref[0], 0, current_row_ref[0], len(columns_order) - 1,
                                         f"{label_text} ({len(df_category)} SRs)", category_label_format)
            current_row_ref[0] += 1

            for _, row_data in df_category.iterrows():
                worksheet.set_row(current_row_ref[0], None, row_color_format)
                for col_num, col_name in enumerate(columns_order):
                    value = row_data.get(col_name)
                    if pd.isna(value):
                        worksheet.write_blank(current_row_ref[0], col_num, '')
                    elif isinstance(value, (datetime, pd.Timestamp)):
                        worksheet.write_string(current_row_ref[0], col_num, value.strftime('%d-%b-%Y'))
                    elif pd.api.types.is_numeric_dtype(type(value)):
                        if float(value) == int(value):
                            worksheet.write_number(current_row_ref[0], col_num, int(value))
                        else:
                            worksheet.write_number(current_row_ref[0], col_num, value)
                    else:
                        worksheet.write_string(current_row_ref[0], col_num, str(value))
                current_row_ref[0] += 1
        return current_row_ref[0]

    current_row_list = [current_row]

    write_category_block_with_label_internal(df_cat1_le_7_sr_counts, "SRs Less Than or Equal to 7 Days", green_row_format, current_row_list, final_sr_counts_columns)
    write_category_block_with_label_internal(df_cat2_gt7_le15_sr_counts, "SRs More Than 7 But Less Than or Equal to 15 Days", yellow_row_format, current_row_list, final_sr_counts_columns)
    write_category_block_with_label_internal(df_cat3_gt_15_sr_counts, "SRs More Than 15 Days", red_row_format, current_row_list, final_sr_counts_columns)
    write_category_block_with_label_internal(df_invalid_sr_counts, "Invalid SRs (Missing SR Creation Date)", invalid_sr_row_format, current_row_list, final_sr_counts_columns) # New block

    if not df_purple_category_sr_counts.empty:
        write_category_block_with_label_internal(df_purple_category_sr_counts, "Unreceived SRs with SR Closed", purple_row_format, current_row_list, final_sr_counts_columns)

    return True # Indicate success

def export_unreceived_meters_report(df_original_data, file_path):
    """
    Exports a report of SRs where the 'GRN date' is empty, categorized by 'Days Passed'.
    Includes a separate 'SR counts' sheet.
    This function expects df_original_data to have already cleaned column names.
    """
    print("DEBUG: Entering export_unreceived_meters_report function.")

    if df_original_data is None or df_original_data.empty:
        print("No data provided for 'Unreceived Meter Data' export.")
        return False

    df_export = df_original_data.copy()

    date_cols_to_format_excel = [
        clean_column_name("SR creation date"),
        clean_column_name("Incident Date"),
        clean_column_name("GRN date"),
        clean_column_name("Repair complete date"),
        clean_column_name("SR closure date"),
        clean_column_name("Inter-org challan date from Branch to Repair center"),
        clean_column_name("Inter-org challan date from Repair center to Branch"),
        clean_column_name("Sales Shipment Date"),
        clean_column_name("Repair closure date")
    ]

    cleaned_sr_creation_date_col = clean_column_name('SR creation date')
    cleaned_grn_date_col = clean_column_name('GRN date')
    cleaned_sr_closure_date_col = clean_column_name('SR closure date')

    if cleaned_sr_creation_date_col not in df_export.columns:
        print(f"Error: Required column '{cleaned_sr_creation_date_col}' not found. Cannot process unreceived items.")
        return False
    if cleaned_grn_date_col not in df_export.columns:
        print(f"Error: Required column '{cleaned_grn_date_col}' not found. Cannot filter by empty GRN dates.")
        return False

    has_sr_closure_date_col = cleaned_sr_closure_date_col in df_export.columns

    for col in date_cols_to_format_excel:
        if col in df_export.columns:
            # Added replace for common empty string variants BEFORE pd.to_datetime
            df_export[col] = df_export[col].replace('', np.nan).replace(' ', np.nan)
            df_export[col] = pd.to_datetime(df_export[col], errors='coerce')


    df_unreceived_meters_report = df_export[df_export[cleaned_grn_date_col].isna()].copy()

    if df_unreceived_meters_report.empty:
        print("No SRs found with empty GRN dates for this report.")
        return False

    today = pd.Timestamp.now()

    # Calculate Days Passed (con date)
    # Records where SR creation date is NaT will result in Days Passed being NaN
    df_unreceived_meters_report['Days Passed'] = np.where(
        df_unreceived_meters_report[cleaned_sr_creation_date_col].notna(),
        (today - df_unreceived_meters_report[cleaned_sr_creation_date_col]).dt.days + 1,
        np.nan # If SR creation date is NaT, set to NaN
    )
    df_unreceived_meters_report['Days Passed'] = df_unreceived_meters_report['Days Passed'].apply(lambda x: max(0, x) if pd.notna(x) else np.nan)

    df_unreceived_meters_report['con date'] = df_unreceived_meters_report['Days Passed']

    def categorize_days_passed(days):
        if pd.isna(days):
            return "Invalid SR" # Changed from "" to "Invalid SR"
        elif days <= 7:
            return "less than or equal to 7 days"
        elif 7 < days <= 15:
            return "more than 7 but less than or equal to 15 days"
        else:
            return "more than 15 days"

    df_unreceived_meters_report['Days Passed Category'] = df_unreceived_meters_report['Days Passed'].apply(categorize_days_passed)

    df_purple_category = pd.DataFrame(columns=df_unreceived_meters_report.columns)
    if has_sr_closure_date_col:
        purple_mask = (df_unreceived_meters_report[cleaned_grn_date_col].isna() &
                       df_unreceived_meters_report[cleaned_sr_closure_date_col].notna())
        df_purple_category = df_unreceived_meters_report[purple_mask].copy()
        df_unreceived_meters_report = df_unreceived_meters_report[~purple_mask].copy()


    df_cat1_le_7 = df_unreceived_meters_report[df_unreceived_meters_report['Days Passed Category'] == "less than or equal to 7 days"].sort_values(by='Days Passed', ascending=True)
    df_cat2_gt7_le15 = df_unreceived_meters_report[df_unreceived_meters_report['Days Passed Category'] == "more than 7 but less than or equal to 15 days"].sort_values(by='Days Passed', ascending=True)
    df_cat3_gt_15 = df_unreceived_meters_report[df_unreceived_meters_report['Days Passed Category'] == "more than 15 days"].sort_values(by='Days Passed', ascending=True)
    df_invalid_category = df_unreceived_meters_report[df_unreceived_meters_report['Days Passed Category'] == "Invalid SR"].copy() # New DataFrame for Invalid SRs

    try:
        writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
        workbook = writer.book

        sr_counts_success = _write_categorized_sr_counts_sheet_tf1(df_original_data, workbook, 'SR counts', date_cols_to_format_excel)
        if not sr_counts_success:
            print("SR counts sheet could not be written. Continuing with main sheet if possible.")

        worksheet = workbook.add_worksheet('Unreceived Meters')

        header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'border': 1, 'align': 'center'})
        date_format = workbook.add_format({'num_format': 'dd-mmm-yyyy'})
        integer_format = workbook.add_format({'num_format': '#,##0'})
        text_format = workbook.add_format({'text_wrap': False})

        green_row_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        yellow_row_format = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})
        red_row_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        purple_row_format = workbook.add_format({'bg_color': '#E2CFF4', 'font_color': '#5B00B7'})
        invalid_sr_row_format = workbook.add_format({'bg_color': '#D3D3D3', 'font_color': '#333333'}) # Light Gray


        category_label_format = workbook.add_format({
            'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter',
            'bg_color': '#ADD8E6', 'font_color': '#000000'
        })
        total_count_format = workbook.add_format({'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter'})

        # MODIFIED LOGIC: Exclude the purple category from the total count
        # Ensure invalid SRs are included in the total if they are part of the 'unreceived' set
        total_unreceived_count = len(df_unreceived_meters_report)
        worksheet.write(0, 0, f"Total Unreceived Records: {total_unreceived_count}", total_count_format)
        current_row = 2

        ordered_cols_for_this_export = [
            clean_column_name("Branch name"),
            clean_column_name("SR no."),
            clean_column_name("SR creation date"),
            "con date",
        ]

        all_possible_cols = [col for col in df_unreceived_meters_report.columns if col not in ordered_cols_for_this_export]

        for col in [
            clean_column_name("Incident Date"), clean_column_name("SR status"),
            clean_column_name("SR Problem type"), clean_column_name("Repair No"),
            clean_column_name("Meter Sr. No."), clean_column_name("Duration at Repair center"),
            clean_column_name("Item description"), clean_column_name("Product family"),
            clean_column_name("GRN date"),
            clean_column_name("Inter-org challan date from Branch to Repair center"),
            clean_column_name("Inter-org challan date from Repair center to Branch"),
            clean_column_name("Sales Shipment Date"),
            "Ageing (Calculated)",
            clean_column_name("Defect in Lot(Repair line)"),
            clean_column_name("Problem Description"), clean_column_name("Problem Investigation"),
            clean_column_name("Diagnose code 1-Category"), clean_column_name("Diagnose code 1-Description"),
            clean_column_name("Diagnose code 2-Category"), clean_column_name("Diagnose code 2-Description"),
            clean_column_name("Diagnose code 3-Category"), clean_column_name("Diagnose code 3-Description"),
            clean_column_name("Service code"),
            clean_column_name("Repair complete date"),
            clean_column_name("Repair closure date"),
            clean_column_name("Customer name"), clean_column_name("SR summary"),
            clean_column_name("Warranty status"), clean_column_name("SR closure date"),
            'Days Passed', 'Days Passed Category',
        ]:
            if col not in ordered_cols_for_this_export and col not in all_possible_cols:
                all_possible_cols.append(col)

        final_export_columns = ordered_cols_for_this_export + all_possible_cols

        df_cat1_le_7 = df_cat1_le_7.reindex(columns=final_export_columns)
        df_cat2_gt7_le15 = df_cat2_gt7_le15.reindex(columns=final_export_columns)
        df_cat3_gt_15 = df_cat3_gt_15.reindex(columns=final_export_columns)
        df_purple_category = df_purple_category.reindex(columns=final_export_columns)
        df_invalid_category = df_invalid_category.reindex(columns=final_export_columns) # Reindex for Invalid SRs

        combined_df_for_type_check = pd.concat([df_cat1_le_7, df_cat2_gt7_le15, df_cat3_gt_15, df_purple_category, df_invalid_category], ignore_index=True) # Include Invalid SRs
        if 'Days Passed' in combined_df_for_type_check.columns:
            combined_df_for_type_check['Days Passed'] = pd.to_numeric(combined_df_for_type_check['Days Passed'], errors='coerce')
        if 'con date' in combined_df_for_type_check.columns:
            combined_df_for_type_check['con date'] = pd.to_numeric(combined_df_for_type_check['con date'], errors='coerce')


        for col_num, col_name in enumerate(final_export_columns):
            worksheet.write(current_row, col_num, col_name, header_format)
        current_row += 1

        for i, col_name in enumerate(final_export_columns):
            current_col_name_cleaned = clean_column_name(col_name)
            if current_col_name_cleaned in date_cols_to_format_excel:
                worksheet.set_column(i, i, 15, date_format)
            elif 'description' in current_col_name_cleaned.lower() or \
                    'summary' in current_col_name_cleaned.lower() or \
                    'problem' in current_col_name_cleaned.lower() or \
                    'investigation' in current_col_name_cleaned.lower():
                worksheet.set_column(i, i, 40, text_format)
            elif current_col_name_cleaned in ['Days Passed', 'con date'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
                worksheet.set_column(i, i, 15, integer_format)
            elif col_name in combined_df_for_type_check.columns and pd.api.types.is_numeric_dtype(combined_df_for_type_check[col_name]):
                worksheet.set_column(i, i, 12, integer_format)
            else:
                worksheet.set_column(i, i, 15)

        def write_category_block_with_label(df_category, label_text, row_color_format, current_row_ref):
            if not df_category.empty:
                if current_row_ref[0] > 1:
                    current_row_ref[0] += 1

                worksheet.merge_range(current_row_ref[0], 0, current_row_ref[0], len(final_export_columns) - 1,
                                             f"{label_text} ({len(df_category)} SRs)", category_label_format)
                current_row_ref[0] += 1

                for _, row_data in df_category.iterrows():
                    worksheet.set_row(current_row_ref[0], None, row_color_format)
                    for col_num, col_name in enumerate(final_export_columns):
                        value = row_data.get(col_name)
                        if pd.isna(value):
                            worksheet.write_blank(current_row_ref[0], col_num, '')
                        elif isinstance(value, (datetime, pd.Timestamp)):
                            worksheet.write_string(current_row_ref[0], col_num, value.strftime('%d-%b-%Y'))
                        elif pd.api.types.is_numeric_dtype(type(value)):
                            if float(value) == int(value):
                                worksheet.write_number(current_row_ref[0], col_num, int(value))
                            else:
                                worksheet.write_number(current_row_ref[0], col_num, value)
                        else:
                            worksheet.write_string(current_row_ref[0], col_num, str(value))
                    current_row_ref[0] += 1
                return current_row_ref[0]

        current_row_list = [current_row]

        write_category_block_with_label(df_cat1_le_7, "Unreceived SRs Less Than or Equal to 7 Days", green_row_format, current_row_list)
        write_category_block_with_label(df_cat2_gt7_le15, "Unreceived SRs More Than 7 But Less Than or Equal to 15 Days", yellow_row_format, current_row_list)
        write_category_block_with_label(df_cat3_gt_15, "Unreceived SRs More Than 15 Days", red_row_format, current_row_list)
        write_category_block_with_label(df_invalid_category, "Invalid SRs (Missing SR Creation Date)", invalid_sr_row_format, current_row_list) # New block

        if not df_purple_category.empty:
            write_category_block_with_label(df_purple_category, "Unreceived SRs with SR Closed", purple_row_format, current_row_list)

        workbook.close()
        print(f"Successfully exported unreceived meters categorized report to: {file_path}")
        time.sleep(0.5)
        return True

    except Exception as e:
        traceback.print_exc()
        print(f"An unexpected error occurred during export: {e}")
        return False