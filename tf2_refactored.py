# tf2_refactored.py (CORRECTED with all 3 rules)

import pandas as pd
import numpy as np
from datetime import datetime
import xlsxwriter
import traceback
import time

# Import the centralized clean_column_name function from the new utility file
from data_cleaning_utils import clean_column_name

# Helper to write a single DataFrame to a specific worksheet with custom date formatting,
# column widths, and header/data writing logic.
def _write_single_df_to_worksheet_tf2(df_to_write, worksheet, workbook, start_row=0):
    """
    Helper to write a single DataFrame to a specific worksheet with custom date formatting,
    column widths, and header/data writing logic.
    `start_row`: The row number to begin writing data (0-indexed).
    Note: Renamed to avoid clash if both tf1 and tf2 use a similarly named internal helper
          and are imported into the same scope, though in this modular design, it's less of an issue.
    """
    date_format = workbook.add_format({'num_format': 'dd-mmm-yyyy'})
    integer_format = workbook.add_format({'num_format': '#,##0'})
    text_format = workbook.add_format({'text_wrap': False})
    header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'border': 1, 'align': 'center'})

    date_cols_to_format = [
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
        elif current_col_name_cleaned == 'Days Passed' or current_col_name_cleaned == 'CON date' or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
            worksheet.set_column(i, i, 15, integer_format)
        elif col_name in df_to_write.columns and pd.api.types.is_numeric_dtype(df_to_write[col_name]):
            worksheet.set_column(i, i, 12, integer_format)
        elif current_col_name_cleaned == 'SR NO':
            worksheet.set_column(i, i, 15)
        elif current_col_name_cleaned == 'Total SR' or current_col_name_cleaned == 'Pending SR':
            worksheet.set_column(i, i, 12, integer_format)
        elif current_col_name_cleaned == 'SPECIFIC METER SERIAL NUMBER':
            worksheet.set_column(i, i, 25)
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
            elif current_col_name_cleaned in ['Days Passed', 'CON date', 'Total SR', 'Pending SR'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
                final_cell_format = integer_format
            elif pd.api.types.is_numeric_dtype(type(value)):
                if float(value) == int(value):
                    worksheet.write_number(excel_row_num, col_num, int(value))
                else:
                    worksheet.write_number(excel_row_num, col_num, value)
            else:
                worksheet.write_string(excel_row_num, col_num, str(value))


def _write_categorized_sr_counts_sheet_tf2(df_filtered_data_tf2, workbook, worksheet_name):
    """
    Prepares and writes the 'SR counts' sheet with unique SRs,
    excluding closed SRs, and categorizing them by 'CON date'.
    This function now expects df_filtered_data_tf2 to be the PRE-FILTERED DataFrame.
    """
    print(f"DEBUG: Preparing '{worksheet_name}' sheet with pre-filtered data.")
    if df_filtered_data_tf2 is None or df_filtered_data_tf2.empty:
        print(f"No data to write to '{worksheet_name}' sheet.")
        # Still create the sheet but show a message
        worksheet = workbook.add_worksheet(worksheet_name)
        worksheet.write(0, 0, "No unrepaired meters found to generate SR Counts.")
        return

    worksheet = workbook.add_worksheet(worksheet_name)

    date_format = workbook.add_format({'num_format': 'dd-mmm-yyyy'})
    integer_format = workbook.add_format({'num_format': '#,##0'})
    text_format = workbook.add_format({'text_wrap': False})
    header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'border': 1, 'align': 'center'})

    green_row_format = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#1E90FF'})
    yellow_row_format = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#1E90FF'})
    red_row_format = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#1E90FF'})
    default_blue_font_format = workbook.add_format({'font_color': '#1E90FF'})

    category_label_format = workbook.add_format({
        'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter',
        'bg_color': '#ADD8E6', 'font_color': '#000000'
    })
    total_count_format = workbook.add_format({'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter'})
    unrepair_label_format = workbook.add_format({'bold': True, 'font_size': 16, 'align': 'left', 'font_color': '#000000'})

    df_to_process = df_filtered_data_tf2.copy()

    cleaned_grn_date_col = clean_column_name('GRN date')
    cleaned_sr_no_col = clean_column_name('SR no.')

    if cleaned_sr_no_col in df_to_process.columns:
        df_to_process = df_to_process.drop_duplicates(subset=[cleaned_sr_no_col]).copy()
    else:
        print(f"Warning: '{cleaned_sr_no_col}' not found for duplicate removal in '{worksheet_name}' sheet. Skipping duplicate removal.")

    if df_to_process.empty:
        print(f"No data remaining after duplicate removal for '{worksheet_name}' sheet.")
        worksheet.write(0, 0, "No unrepaired meters found to generate SR Counts.")
        return

    # 'CON date' should already be calculated in the main function and present.
    if 'CON date' not in df_to_process.columns:
         print(f"CRITICAL ERROR: 'CON date' is missing from the pre-filtered data passed to _write_categorized_sr_counts_sheet_tf2.")
         worksheet.write(0, 0, "Error: CON date calculation failed.")
         return

    def categorize_sr_days_for_excel(sr_days):
        if pd.isna(sr_days):
            return "No GRN Date / Invalid SR Days"
        elif sr_days <= 7:
            return 'SRs Less Than or Equal to 7 Days'
        elif 7 < sr_days <= 15:
            return 'SRs Between 7 and 15 Days'
        elif sr_days > 15:
            return 'SRs More Than 15 Days'
        else:
            return 'Other / Uncategorized'

    df_to_process['SR_Category_For_Report'] = df_to_process['CON date'].apply(categorize_sr_days_for_excel)

    canonical_display_order_for_sr_counts = [
        clean_column_name("Branch name"), clean_column_name("SR no."), clean_column_name("SR creation date"),
        "CON date", clean_column_name("Incident Date"), clean_column_name("SR status"),
        clean_column_name("SR Problem type"), clean_column_name("Repair No"), clean_column_name("Meter Sr. No."),
        clean_column_name("Duration at Repair center"), clean_column_name("Item description"), clean_column_name("Product family"),
        clean_column_name("GRN date"),
        clean_column_name("Inter-org challan date from Branch to Repair center"),
        clean_column_name("Inter-org challan date from Repair center to Branch"),
        clean_column_name("Sales Shipment Date"),
        clean_column_name("Ageing (Calculated)"), clean_column_name("Defect in Lot(Repair line)"),
        clean_column_name("Problem Description"), clean_column_name("Problem Investigation"),
        clean_column_name("Diagnose code 1-Category"), clean_column_name("Diagnose code 1-Description"),
        clean_column_name("Diagnose code 2-Category"), clean_column_name("Diagnose code 2-Description"),
        clean_column_name("Diagnose code 3-Category"), clean_column_name("Diagnose code 3-Description"),
        clean_column_name("Service code"),
        clean_column_name("Repair complete date"),
        clean_column_name("Repair closure date"),
        clean_column_name("Customer name"), clean_column_name("SR summary"),
        clean_column_name("Warranty status"), clean_column_name("SR closure date"),
        'SR_Category_For_Report'
    ]
    final_sr_counts_columns = [col for col in canonical_display_order_for_sr_counts if col in df_to_process.columns]
    df_to_process = df_to_process.reindex(columns=final_sr_counts_columns)

    category_excel_order = [
        'SRs Less Than or Equal to 7 Days',
        'SRs Between 7 and 15 Days',
        'SRs More Than 15 Days',
        'No GRN Date / Invalid SR Days',
        'Other / Uncategorized'
    ]

    current_row = 0

    worksheet.write(0, 0, "UNREPAIR METERS", unrepair_label_format)
    current_row = 2

    total_sr_count = len(df_to_process)
    worksheet.write(current_row, 0, f"Total Open SRs (Unique): {total_sr_count}", total_count_format)
    current_row += 2

    for col_num, col_name in enumerate(final_sr_counts_columns):
        if col_name != 'SR_Category_For_Report':
            worksheet.write(current_row, col_num, col_name, header_format)
        else:
            worksheet.write(current_row, col_num, '', header_format)
    current_row += 1

    date_cols_to_format_local = [
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

    for i, col_name in enumerate(final_sr_counts_columns):
        current_col_name_cleaned = clean_column_name(col_name)
        if current_col_name_cleaned in date_cols_to_format_local:
            worksheet.set_column(i, i, 15, date_format)
        elif 'description' in current_col_name_cleaned.lower() or \
                'summary' in current_col_name_cleaned.lower() or \
                'problem' in current_col_name_cleaned.lower() or \
                'investigation' in current_col_name_cleaned.lower():
            worksheet.set_column(i, i, 40, text_format)
        elif current_col_name_cleaned in ['Days Passed', 'CON date'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
            worksheet.set_column(i, i, 15, integer_format)
        elif col_name in df_to_process.columns and pd.api.types.is_numeric_dtype(df_to_process[col_name]):
            worksheet.set_column(i, i, 12, integer_format)
        else:
            worksheet.set_column(i, i, 15)

    def write_category_block_with_label_internal(df_category, label_text, row_color_format, current_row_ref, columns_order):
        if not df_category.empty:
            current_row_ref[0] += 1
            worksheet.merge_range(current_row_ref[0], 0, current_row_ref[0], len(columns_order) - 1,
                                        f"{label_text} ({len(df_category)} SRs)", category_label_format)
            current_row_ref[0] += 1

            for _, row_data in df_category.iterrows():
                worksheet.set_row(current_row_ref[0], None, row_color_format)
                for col_num, col_name in enumerate(columns_order):
                    if col_name == 'SR_Category_For_Report':
                        worksheet.write_blank(current_row_ref[0], col_num, '')
                        continue

                    value = row_data.get(col_name)
                    if pd.isna(value):
                        worksheet.write_blank(current_row_ref[0], col_num, '')
                    elif isinstance(value, (datetime, pd.Timestamp)):
                        if pd.notna(value):
                            worksheet.write_string(current_row_ref[0], col_num, value.strftime('%d-%b-%Y'))
                        else:
                            worksheet.write_blank(current_row_ref[0], col_num, '', '')
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

    for category in category_excel_order:
        sub_df = df_to_process[df_to_process['SR_Category_For_Report'] == category].copy()
        if not sub_df.empty:
            if category == 'SRs Less Than or Equal to 7 Days':
                row_format_to_use = green_row_format
            elif category == 'SRs Between 7 and 15 Days':
                row_format_to_use = yellow_row_format
            elif category == 'SRs More Than 15 Days':
                row_format_to_use = red_row_format
            else:
                row_format_to_use = default_blue_font_format

            write_category_block_with_label_internal(sub_df, category, row_format_to_use, current_row_list, final_sr_counts_columns)
            current_row_list[0] += 1

    print(f"DEBUG: Finished writing '{worksheet_name}' sheet.")
    return True

def _write_m1_categorized_sheet_tf2(df_filtered_data_tf2, workbook, worksheet_name):
    """
    Prepares and writes the 'M1' sheet.
    This function now expects df_filtered_data_tf2 to be the PRE-FILTERED DataFrame.
    """
    print(f"DEBUG: Preparing '{worksheet_name}' sheet with pre-filtered data.")
    if df_filtered_data_tf2 is None or df_filtered_data_tf2.empty:
        print(f"No original data to write to '{worksheet_name}' sheet.")
        worksheet = workbook.add_worksheet(worksheet_name)
        worksheet.write(0, 0, "No unrepaired meters found to generate M1 report.")
        return False

    worksheet = workbook.add_worksheet(worksheet_name)

    integer_format = workbook.add_format({'num_format': '#,##0'})
    header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'border': 1, 'align': 'center'})

    green_row_format_m1 = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#000000'})
    yellow_row_format_m1 = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#000000'})
    red_row_format_m1 = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#000000'})
    default_row_format_m1 = workbook.add_format({'font_color': '#000000'})

    category_label_format = workbook.add_format({
        'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter',
        'bg_color': '#ADD8E6', 'font_color': '#000000'
    })
    total_count_format = workbook.add_format({'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter'})
    unrepair_label_format = workbook.add_format({'bold': True, 'font_size': 16, 'align': 'left', 'font_color': '#000000'})

    df_m1_processed_for_categorization = df_filtered_data_tf2.copy()
    
    cleaned_grn_date_col = clean_column_name('GRN date')
    cleaned_sr_no_col = clean_column_name('SR no.')
    cleaned_sr_status_col = clean_column_name('SR status')
    cleaned_meter_sr_no_col = clean_column_name('Meter Sr. No.')

    if df_m1_processed_for_categorization.empty:
        print(f"No data remaining for '{worksheet_name}' sheet. Exiting write operation.")
        worksheet.write(0, 0, "No data available for M1 sheet based on current filters.", total_count_format)
        return False

    if 'CON date' not in df_m1_processed_for_categorization.columns:
         print(f"CRITICAL ERROR: 'CON date' is missing from the pre-filtered data passed to _write_m1_categorized_sheet_tf2.")
         worksheet.write(0, 0, "Error: CON date calculation failed.")
         return False

    def categorize_sr_days_for_m1_excel(sr_days, grn_date_val):
        # Since the main filter already ensures GRN date is present, we simplify this
        if pd.isna(sr_days):
            return "Invalid SR Days" # Should not happen with corrected logic
        elif sr_days <= 7:
            return 'SRs Less Than or Equal to 7 Days'
        elif 7 < sr_days <= 15:
            return 'SRs Between 7 and 15 Days'
        elif sr_days > 15:
            return 'SRs More Than 15 Days'
        else:
            return 'Other / Uncategorized'

    df_m1_processed_for_categorization['SR_Category_For_Report'] = df_m1_processed_for_categorization.apply(
        lambda row: categorize_sr_days_for_m1_excel(row['CON date'], row[cleaned_grn_date_col]), axis=1
    )

    # Note: This logic for Total/Pending SR might need adjustment if it's supposed to come from the *original* unfiltered data.
    # For now, it will calculate based on the pre-filtered "unrepaired" data.
    sr_summary_counts = df_m1_processed_for_categorization.groupby(cleaned_sr_no_col).agg(
        total_sr_per_group=(cleaned_sr_no_col, 'size'),
        pending_sr_per_group=(cleaned_sr_status_col, lambda x: (x.astype(str).str.strip().str.lower() == 'open').sum())
    ).reset_index()
    sr_summary_counts = sr_summary_counts.set_index(cleaned_sr_no_col)


    category_excel_order = [
        'SRs Less Than or Equal to 7 Days',
        'SRs Between 7 and 15 Days',
        'SRs More Than 15 Days',
        'Invalid SR Days',
        'Other / Uncategorized'
    ]
    
    m1_display_columns = ['SR NO', 'Total SR', 'Pending SR', 'SPECIFIC METER SERIAL NUMBER']

    current_row_idx = 0

    worksheet.write(current_row_idx, 0, "UNREPAIR METERS", unrepair_label_format)
    current_row_idx += 2

    total_m1_records = len(df_m1_processed_for_categorization)
    worksheet.write(current_row_idx, 0, f"Total Records: {total_m1_records}", total_count_format)
    current_row_idx += 2

    for col_num, value in enumerate(m1_display_columns):
        worksheet.write(current_row_idx, col_num, value, header_format)
    current_row_idx += 1

    for i, col_name in enumerate(m1_display_columns):
        if col_name == 'SR NO':
            worksheet.set_column(i, i, 15)
        elif col_name in ['Total SR', 'Pending SR']:
            worksheet.set_column(i, i, 12, integer_format)
        elif col_name == 'SPECIFIC METER SERIAL NUMBER':
            worksheet.set_column(i, i, 25)
        else:
            worksheet.set_column(i, i, 15)

    def write_m1_category_block(df_category, label_text, row_color_map, current_row_ref, columns_order, sr_summary_data):
        if not df_category.empty:
            current_row_ref[0] += 1
            worksheet.merge_range(current_row_ref[0], 0, current_row_ref[0], len(columns_order) - 1,
                                        f"{label_text} ({len(df_category)} Meters)", category_label_format)
            current_row_ref[0] += 1

            for _, row_data in df_category.iterrows():
                if label_text == 'SRs Less Than or Equal to 7 Days':
                    final_cell_format = row_color_map['green']
                elif label_text == 'SRs Between 7 and 15 Days':
                    final_cell_format = row_color_map['yellow']
                elif label_text == 'SRs More Than 15 Days':
                    final_cell_format = row_color_map['red']
                else:
                    final_cell_format = row_color_map['default']

                current_sr_no = row_data[cleaned_sr_no_col]
                total_sr_count = sr_summary_data.loc[current_sr_no, 'total_sr_per_group'] if current_sr_no in sr_summary_data.index else 0
                pending_sr_count = sr_summary_data.loc[current_sr_no, 'pending_sr_per_group'] if current_sr_no in sr_summary_data.index else 0

                values_to_write = {
                    'SR NO': current_sr_no,
                    'Total SR': total_sr_count,
                    'Pending SR': pending_sr_count,
                    'SPECIFIC METER SERIAL NUMBER': str(row_data.get(cleaned_meter_sr_no_col, ''))
                }

                for col_num, col_name_to_write in enumerate(columns_order):
                    value = values_to_write.get(col_name_to_write, '')
                    if pd.isna(value) or value == '':
                        worksheet.write_blank(current_row_ref[0], col_num, '', final_cell_format)
                    elif isinstance(value, (int, float, np.integer, np.floating)):
                        if float(value) == int(value):
                            worksheet.write_number(current_row_ref[0], col_num, int(value), final_cell_format)
                        else:
                            worksheet.write_number(current_row_ref[0], col_num, value, final_cell_format)
                    else:
                        worksheet.write_string(current_row_ref[0], col_num, str(value), final_cell_format)
                current_row_ref[0] += 1
            current_row_ref[0] += 1
        return current_row_ref[0]

    current_row_list = [current_row_idx]

    m1_row_colors = {
        'green': green_row_format_m1,
        'yellow': yellow_row_format_m1,
        'red': red_row_format_m1,
        'default': default_row_format_m1
    }

    for category in category_excel_order:
        sub_df = df_m1_processed_for_categorization[df_m1_processed_for_categorization['SR_Category_For_Report'] == category].copy()
        write_m1_category_block(sub_df, category, m1_row_colors, current_row_list, m1_display_columns, sr_summary_counts)

    print(f"DEBUG: Finished writing '{worksheet_name}' sheet.")
    return True # Indicate success


# Main export function to be called from ex1.py
def export_unrepaired_meters_report(df_original_data, file_path):
    """
    Generates and exports the comprehensive Unrepaired Meters Report.
    This includes 'SR counts', 'M1', and 'SR Summary Report' sheets.
    This function expects df_original_data to have already cleaned column names.
    """
    print("DEBUG: Entering export_unrepaired_meters_report function.")

    if df_original_data is None or df_original_data.empty:
        print("No data provided for 'Unrepaired Meters' export.")
        return False

    df_report_data = df_original_data.copy()

    # Ensure date columns are datetime for calculations early on
    relevant_date_cols = [
        clean_column_name("SR creation date"),
        clean_column_name("GRN date"),
        clean_column_name("Repair complete date"),
        clean_column_name("Sales Shipment Date")
    ]
    for col in relevant_date_cols:
        if col in df_report_data.columns:
            df_report_data[col] = pd.to_datetime(df_report_data[col], errors='coerce')

    cleaned_con_date_col = 'CON date'
    cleaned_sr_status_col = clean_column_name("SR status")
    cleaned_grn_date_col = clean_column_name("GRN date")
    cleaned_repair_complete_date_col = clean_column_name("Repair complete date")
    
    # --- NEW FILTER LOGIC based on all 3 user rules ---
    print("Applying new 3-part filter: (Status != Closed) AND (GRN date is present) AND (Repair date is absent).")
    
    required_filter_cols = [cleaned_sr_status_col, cleaned_grn_date_col, cleaned_repair_complete_date_col]
    if not all(col in df_report_data.columns for col in required_filter_cols):
        print(f"Error: Required columns for filtering are missing. Needs: {required_filter_cols}")
        return False
        
    # 1. SR status is not 'Closed'
    mask_status = df_report_data[cleaned_sr_status_col].astype(str).str.strip().str.lower() != 'closed'
    # 2. GRN date is present
    mask_grn_present = df_report_data[cleaned_grn_date_col].notna()
    # 3. Repair complete date is absent
    mask_repair_absent = df_report_data[cleaned_repair_complete_date_col].isna()
    
    # Combine all three masks
    final_mask = mask_status & mask_grn_present & mask_repair_absent
    
    df_report_data = df_report_data[final_mask].copy()
    print(f"DEBUG: Found {len(df_report_data)} unrepaired records after applying the 3-part filter.")


    today_for_calc = datetime.now()
    # Calculate 'CON date' using GRN date. We already know GRN date is not null from the filter above.
    df_report_data['CON date'] = (today_for_calc - df_report_data[cleaned_grn_date_col]).dt.days + 1
    df_report_data['CON date'] = df_report_data['CON date'].apply(lambda x: max(0, x) if pd.notna(x) else np.nan)


    if cleaned_con_date_col not in df_report_data.columns or df_report_data[cleaned_con_date_col].isnull().all():
        print("The 'CON date' column could not be calculated correctly. Please ensure 'GRN date' is present and valid.")
        
    try:
        writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
        workbook = writer.book

        # Pass the CORRECTLY FILTERED data to the helper functions.
        sr_counts_success = _write_categorized_sr_counts_sheet_tf2(df_report_data, workbook, 'SR counts')
        m1_success = _write_m1_categorized_sheet_tf2(df_report_data, workbook, 'M1')

        if not sr_counts_success and not m1_success:
             print("Neither SR counts nor M1 sheet could be written. Aborting main report.")
             workbook.close()
             return False

        sheet = workbook.add_worksheet('SR Summary Report')

        header_format = workbook.add_format({'bold': True, 'text_wrap': True, 'valign': 'top', 'border': 1})
        default_cell_format = workbook.add_format({})
        category_title_format = workbook.add_format({'bold': True, 'font_size': 14, 'align': 'left'})
        total_count_format = workbook.add_format({'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter'})
        unrepair_label_format = workbook.add_format({'bold': True, 'font_size': 16, 'align': 'left', 'font_color': '#000000'})

        green_bg_format = workbook.add_format({'bg_color': '#C6EFCE'})
        yellow_bg_format = workbook.add_format({'bg_color': '#FFEB9C'})
        red_bg_format = workbook.add_format({'bg_color': '#FFC7CE'})

        open_green_format = workbook.add_format({'font_color': '#1E90FF', 'bg_color': '#C6EFCE'})
        open_yellow_format = workbook.add_format({'font_color': '#1E90FF', 'bg_color': '#FFEB9C'})
        open_red_format = workbook.add_format({'font_color': '#1E90FF', 'bg_color': '#FFC7CE'})
        open_default_format = workbook.add_format({'font_color': '#1E90FF'})

        sheet.write(0, 0, "UNREPAIR METERS", unrepair_label_format)

        total_report_count = len(df_report_data)
        sheet.write(2, 0, f"Total Records in Report: {total_report_count}", total_count_format)
        current_row = 4

        output_columns_summary = [
            clean_column_name("Branch name"),
            clean_column_name("SR no."),
            clean_column_name("Meter Sr. No."),
            clean_column_name("SR creation date"),
            "CON date",
            clean_column_name("GRN date"),
            clean_column_name("SR Problem type"),
            clean_column_name("Repair No"),
            clean_column_name("Product family"),
            clean_column_name("Problem Description"),
            clean_column_name("SR summary"),
            clean_column_name("Sales Shipment Date")
        ]

        output_columns_summary = [col for col in output_columns_summary if col in df_report_data.columns]

        for col_num, value in enumerate(output_columns_summary):
            sheet.write(current_row, col_num, value, header_format)
        current_row += 1

        row_start = current_row

        def categorize_sr_days_for_summary_excel(sr_days):
            if pd.isna(sr_days):
                return "No GRN Date / Invalid SR Days"
            elif sr_days <= 7:
                return 'SRs Less Than or Equal to 7 Days'
            elif 7 < sr_days <= 15:
                return 'SRs Between 7 and 15 Days'
            elif sr_days > 15:
                return 'SRs More Than 15 Days'
            else:
                return 'Other / Uncategorized'

        df_report_data['SR_Category_For_Report'] = df_report_data['CON date'].apply(categorize_sr_days_for_summary_excel)

        df_report_data['sort_key'] = df_report_data[cleaned_sr_status_col].apply(
            lambda x: 0 if str(x).strip().lower() == 'open' else 1
        )
        category_excel_order = [
            'SRs Less Than or Equal to 7 Days',
            'SRs Between 7 and 15 Days',
            'SRs More Than 15 Days',
            'No GRN Date / Invalid SR Days',
            'Other / Uncategorized'
        ]
        df_report_data['SR_Category_For_Report'] = pd.Categorical(
            df_report_data['SR_Category_For_Report'],
            categories=category_excel_order,
            ordered=True
        )

        df_report_data = df_report_data.sort_values(
            by=['sort_key', 'SR_Category_For_Report', cleaned_con_date_col, 'SR no.'],
            ascending=[True, True, False, True]
        )
        df_report_data = df_report_data.drop(columns=['sort_key'])

        def write_summary_category_block(df_category, label_prefix, row_color_map, current_row_ref, columns_order):
            if not df_category.empty:
                current_row_ref[0] += 1
                sheet.write(current_row_ref[0], 0, label_prefix, category_title_format)
                current_row_ref[0] += 2

                for category in category_excel_order:
                    sub_df = df_category[df_category['SR_Category_For_Report'] == category].copy()
                    if not sub_df.empty:
                        sheet.write(current_row_ref[0], 0, f"{category} ({len(sub_df)} SRs)", category_title_format)
                        current_row_ref[0] += 1

                        for index, row in sub_df.iterrows():
                            if category == 'SRs Less Than or Equal to 7 Days':
                                final_cell_format = row_color_map['green']
                            elif category == 'SRs Between 7 and 15 Days':
                                final_cell_format = row_color_map['yellow']
                            elif category == 'SRs More Than 15 Days':
                                final_cell_format = row_color_map['red']
                            else:
                                final_cell_format = row_color_map['default']

                            for col_num, col_name_to_write in enumerate(columns_order):
                                cell_value = row.get(col_name_to_write, '')
                                if pd.isna(cell_value):
                                    cell_value = ""
                                elif isinstance(cell_value, pd.Timestamp):
                                    cell_value = cell_value.strftime('%d-%b-%Y')
                                sheet.write(current_row_ref[0], col_num, cell_value, final_cell_format)
                            current_row_ref[0] += 1
                        current_row_ref[0] += 1
                current_row_ref[0] += 1
            return current_row_ref[0]

        row_ref_list = [row_start]

        open_srs_df = df_report_data[df_report_data[cleaned_sr_status_col].astype(str).str.strip().str.lower() == 'open'].copy()
        open_colors = {
            'green': open_green_format,
            'yellow': open_yellow_format,
            'red': open_red_format,
            'default': open_default_format
        }
        write_summary_category_block(open_srs_df, "Open SRs", open_colors, row_ref_list, output_columns_summary)

        other_srs_df = df_report_data[df_report_data[cleaned_sr_status_col].astype(str).str.strip().str.lower() != 'open'].copy()
        other_colors = {
            'green': green_bg_format,
            'yellow': yellow_bg_format,
            'red': red_bg_format,
            'default': default_cell_format
        }
        write_summary_category_block(other_srs_df, "Other SRs (Not Open)", other_colors, row_ref_list, output_columns_summary)

        workbook.close()
        print("SR Summary Report exported successfully.")
        time.sleep(0.5)
        return True

    except Exception as e:
        traceback.print_exc()
        print(f"Failed to export SR Summary Report:\n{e}")
        return False