import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import xlsxwriter
import traceback

# Import the centralized clean_column_name function
# Assuming data_cleaning_utils.py exists and has clean_column_name
try:
    from data_cleaning_utils import clean_column_name
except ImportError:
    print("WARNING: data_cleaning_utils.py not found. Using a dummy clean_column_name function.")
    def clean_column_name(col_name):
        return col_name.strip().replace('.', '').replace('-', '').replace(' ', '').lower()


# Suppress all FutureWarnings from Pandas to clean up console
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- Excel Export Helper (used by multiple export functions) ---
def _tf3_export_df_to_excel_with_formatting(df_to_export, worksheet, workbook, start_row=0):
    """
    Helper to export DataFrame to Excel with custom date formatting,
    column widths. This is a general helper and does NOT handle row-level
    background colors for specific categories, as that is managed by the
    calling export functions directly for more fine-grained control.
    It now takes an existing worksheet object and workbook object.
    """
    try:
        # Define cell format properties (dictionaries)
        date_format_props = {'num_format': 'dd-mmm-yyyy'}
        integer_format_props = {'num_format': '#,##0'}
        text_format_props = {'text_wrap': False}

        # Create actual format objects for column setting
        date_format = workbook.add_format(date_format_props)
        integer_format = workbook.add_format(integer_format_props)
        text_format = workbook.add_format(text_format_props)


        # Columns that are expected to be dates
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
        
        # Write headers
        header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'border': 1, 'align': 'center'})
        for col_num, value in enumerate(df_to_export.columns.values):
            worksheet.write(start_row, col_num, value, header_format)

        # Apply column widths (only once per column)
        for i, col_name in enumerate(df_to_export.columns):
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
            # Check for pandas nullable integer dtype
            elif pd.api.types.is_integer_dtype(df_to_export[col_name]):
                worksheet.set_column(i, i, 12, integer_format)
            # General numeric check for float or other numbers
            elif pd.api.types.is_numeric_dtype(df_to_export[col_name]):
                worksheet.set_column(i, i, 12) # No specific format, default number format
            else:
                worksheet.set_column(i, i, 15)

        # Write data rows with explicit type handling for dates and numbers
        for row_num, (index, row_data) in enumerate(df_to_export.iterrows()):
            excel_row_num = row_num + 1 + start_row # Excel is 1-indexed for rows, plus start_row offset

            for col_num, col_name in enumerate(df_to_export.columns):
                value = row_data[col_name]
                
                # Apply general cell format based on content
                final_cell_format = text_format # Default to text format
                current_col_name_cleaned = clean_column_name(col_name)

                if current_col_name_cleaned in date_cols_to_format:
                    final_cell_format = date_format
                elif current_col_name_cleaned in ['Days Passed', 'con date'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
                    final_cell_format = integer_format
                elif pd.api.types.is_numeric_dtype(type(value)): # Check type of actual value, not column
                    final_cell_format = integer_format
                
                if pd.isna(value):
                    worksheet.write_blank(excel_row_num, col_num, '', final_cell_format)
                elif isinstance(value, (datetime, pd.Timestamp)):
                    if pd.notna(value):
                        worksheet.write_datetime(excel_row_num, col_num, value, final_cell_format) # Use write_datetime
                    else:
                        worksheet.write_blank(excel_row_num, col_num, '', final_cell_format)
                elif pd.api.types.is_numeric_dtype(type(value)): # Re-check numeric type
                    if float(value) == int(value): 
                        worksheet.write_number(excel_row_num, col_num, int(value), final_cell_format)
                    else: 
                        worksheet.write_number(excel_row_num, col_num, value, final_cell_format)
                else: 
                    worksheet.write_string(excel_row_num, col_num, str(value), final_cell_format)
        
        return True
    except Exception as e:
        traceback.print_exc()
        print(f"Error exporting sheet '{worksheet.name}': {e}")
        return False

def _tf3_export_unclosed_repair_lines_by_repair_closer(df_original_data, workbook):
    """
    Exports a report of repair lines where 'SR status' is not 'Closed'
    and 'Repair complete date' is available, to a separate sheet "By Repair Closer".
    The 'Inter-org challan date from Repair center to Branch' is NOT a filter here.
    Calculates 'Days Passed' and categorizes similarly to the main sheet.
    It takes df_original_data and an existing workbook object.
    Returns the processed DataFrame (even if empty) or None on critical failure.
    """
    print("DEBUG: Entering _tf3_export_unclosed_repair_lines_by_repair_closer function.")

    df_export = df_original_data.copy()

    cleaned_repair_complete_date_col = clean_column_name('Repair complete date')
    cleaned_sr_status_col = clean_column_name('SR status')

    # Check for required columns
    if cleaned_repair_complete_date_col not in df_export.columns:
        print(f"WARNING: Required column '{cleaned_repair_complete_date_col}' not found for 'By Repair Closer' sheet. Skipping this sheet.")
        return None
    if cleaned_sr_status_col not in df_export.columns:
        print(f"WARNING: Required column '{cleaned_sr_status_col}' not found for 'By Repair Closer' sheet. Skipping this sheet.")
        return None

    # Explicitly ensure relevant date columns are datetime objects before filtering and calculation
    df_export[cleaned_repair_complete_date_col] = pd.to_datetime(df_export[cleaned_repair_complete_date_col], errors='coerce')

    # Filter: SR status is NOT 'Closed' AND Repair complete date is NOT NaN
    # Removed cleaned_inter_org_challan_rc_branch_date_col.notna()
    df_by_repair_closer = df_export[
        (df_export[cleaned_sr_status_col].astype(str).str.lower() != 'closed') &
        df_export[cleaned_repair_complete_date_col].notna()
    ].copy()

    # Get total record count for this sheet
    total_records_rc = len(df_by_repair_closer)

    if df_by_repair_closer.empty:
        print("DEBUG: No data for 'By Repair Closer' sheet based on filter criteria.")
        return df_by_repair_closer # Return empty DataFrame so SR Count can handle it

    today = pd.Timestamp.now()

    # Calculate 'Days Passed' since 'Repair complete date' (inclusive day count: +1)
    df_by_repair_closer['Days Passed'] = (today - df_by_repair_closer[cleaned_repair_complete_date_col]).dt.days + 1
    df_by_repair_closer['Days Passed'] = df_by_repair_closer['Days Passed'].apply(lambda x: max(0, x) if pd.notna(x) else np.nan)
    df_by_repair_closer['Days Passed'] = df_by_repair_closer['Days Passed'].astype('Int64')

    # Calculate 'con date' for this sheet: today's date - repair complete date (inclusive day count: +1)
    df_by_repair_closer['con date'] = (today - df_by_repair_closer[cleaned_repair_complete_date_col]).dt.days + 1
    df_by_repair_closer['con date'] = df_by_repair_closer['con date'].apply(lambda x: max(0, x) if pd.notna(x) else np.nan)
    df_by_repair_closer['con date'] = df_by_repair_closer['con date'].astype('Int64')

    # Add 'Days Passed Category' column for more descriptive analysis
    def categorize_days_passed(days):
        if pd.isna(days):
            return ""
        elif days <= 7:
            return "less than or equal to 7 days"
        elif 7 < days <= 15:
            return "more than 7 but less than or equal to 15 days"
        else:
            return "more than 15 days"
            
    df_by_repair_closer['Days Passed Category'] = df_by_repair_closer['Days Passed'].apply(categorize_days_passed)

    # Define the ABSOLUTE desired column order for THIS SPECIFIC EXPORT.
    ordered_cols_for_this_export = [
        clean_column_name("Branch name"),
        clean_column_name("SR no."),
        clean_column_name("SR creation date"),
        "con date", # Explicitly Column D
    ]

    all_possible_cols = [col for col in df_by_repair_closer.columns if col not in ordered_cols_for_this_export]
    for col in [
        clean_column_name("Incident Date"), clean_column_name("SR status"),
        clean_column_name("SR Problem type"), clean_column_name("Repair No"),
        clean_column_name("Meter Sr. No."), clean_column_name("Duration at Repair center"),
        clean_column_name("Item description"), clean_column_name("Product family"),
        clean_column_name("GRN date"),
        clean_column_name("Inter-org challan date from Branch to Repair center"),
        clean_column_name("Inter-org challan date from Repair center to Branch"),
        clean_column_name("Sales Shipment Date"),
        'Ageing (Calculated)', # Ensure this is in the list
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

    final_export_columns = ordered_cols_for_this_export + [col for col in all_possible_cols if col not in ordered_cols_for_this_export]
    df_by_repair_closer = df_by_repair_closer.reindex(columns=[col for col in final_export_columns if col in df_by_repair_closer.columns])

    worksheet = workbook.add_worksheet('By Repair Complete Date')

    header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'border': 1, 'align': 'center'})
    
    # Base format properties (dictionaries)
    date_format_props_base = {'num_format': 'dd-mmm-yyyy'}
    integer_format_props_base = {'num_format': '#,##0'}
    text_format_props_base = {'text_wrap': False}
    
    # Create actual base format objects for column setting
    date_format = workbook.add_format(date_format_props_base)
    integer_format = workbook.add_format(integer_format_props_base)
    text_format = workbook.add_format(text_format_props_base)

    # Define color properties (dictionaries)
    green_color_props = {'bg_color': '#C6EFCE', 'font_color': '#006100'}
    yellow_color_props = {'bg_color': '#FFEB9C', 'font_color': '#9C6500'}
    red_color_props = {'bg_color': '#FFC7CE', 'font_color': '#9C0006'}

    category_label_format = workbook.add_format({
        'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter',
        'bg_color': '#ADD8E6', 'font_color': '#000000'
    })

    total_count_format = workbook.add_format({'bold': True, 'font_size': 11})
    
    current_row = 0
    worksheet.write(current_row, 0, f"Total Records: {total_records_rc}", total_count_format)
    current_row += 2 # Leave a blank row

    # Write main column headers for this sheet
    for col_num, col_name in enumerate(df_by_repair_closer.columns): # Use df_by_repair_closer's actual columns
        worksheet.write(current_row, col_num, col_name, header_format)
    current_row += 1

    # Set column widths based on the actual columns in the DataFrame being written
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
    for i, col_name in enumerate(df_by_repair_closer.columns): # Use df_by_repair_closer's actual columns
        current_col_name_cleaned = clean_column_name(col_name)
        if current_col_name_cleaned in date_cols_to_format_local:
            worksheet.set_column(i, i, 15, date_format)
        elif 'description' in current_col_name_cleaned.lower() or \
             'summary' in current_col_name_cleaned.lower() or \
             'problem' in current_col_name_cleaned.lower() or \
             'investigation' in current_col_name_cleaned.lower():
            worksheet.set_column(i, i, 40, text_format)
        elif current_col_name_cleaned in ['Days Passed', 'con date'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
            worksheet.set_column(i, i, 15, integer_format)
        elif pd.api.types.is_numeric_dtype(df_by_repair_closer[col_name]):
            worksheet.set_column(i, i, 12, integer_format)
        else:
            worksheet.set_column(i, i, 15)

    # Helper to write a block of categorized data including the new label row for this sheet
    def write_category_block_with_label_sheet2(df_category, label_text, color_properties, current_row_ref, worksheet_obj, final_cols, content_start_row):
        if not df_category.empty:
            if current_row_ref[0] > (content_start_row):
                current_row_ref[0] += 1
            
            worksheet_obj.merge_range(current_row_ref[0], 0, current_row_ref[0], len(final_cols) - 1,
                                     label_text, category_label_format)
            current_row_ref[0] += 1

            for _, row_data in df_category.iterrows():
                date_format_colored = workbook.add_format({**date_format_props_base, **color_properties})
                integer_format_colored = workbook.add_format({**integer_format_props_base, **color_properties})
                text_format_colored = workbook.add_format({**text_format_props_base, **color_properties})

                for col_num, col_name in enumerate(final_cols):
                    value = row_data.get(col_name)
                    current_col_name_cleaned = clean_column_name(col_name)

                    if current_col_name_cleaned in date_cols_to_format_local:
                        cell_format = date_format_colored
                    elif current_col_name_cleaned in ['Days Passed', 'con date'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
                        cell_format = integer_format_colored
                    elif pd.api.types.is_numeric_dtype(type(value)):
                        cell_format = integer_format_colored
                    else:
                        cell_format = text_format_colored

                    if pd.isna(value):
                        worksheet_obj.write_blank(current_row_ref[0], col_num, '', cell_format)
                    elif isinstance(value, (datetime, pd.Timestamp)):
                        worksheet_obj.write_datetime(current_row_ref[0], col_num, value, cell_format)
                    elif pd.api.types.is_numeric_dtype(type(value)):
                        if float(value) == int(value): 
                            worksheet_obj.write_number(current_row_ref[0], col_num, int(value), cell_format)
                        else: 
                            worksheet_obj.write_number(current_row_ref[0], col_num, value, cell_format)
                    else: 
                        worksheet_obj.write_string(current_row_ref[0], col_num, str(value), cell_format)
                current_row_ref[0] += 1
        return current_row_ref[0]

    # Categorize data for 'By Repair Closer' sheet
    df_cat1_le_7_rc = df_by_repair_closer[df_by_repair_closer['Days Passed Category'] == "less than or equal to 7 days"].sort_values(by='Days Passed', ascending=True)
    df_cat2_gt7_le15_rc = df_by_repair_closer[df_by_repair_closer['Days Passed Category'] == "more than 7 but less than or equal to 15 days"].sort_values(by='Days Passed', ascending=True)
    df_cat3_gt_15_rc = df_by_repair_closer[df_by_repair_closer['Days Passed Category'] == "more than 15 days"].sort_values(by='Days Passed', ascending=True)

    current_row_list_rc = [current_row]
    content_start_row_rc_sheet = current_row

    # Write category blocks for "By Repair Closer" sheet
    write_category_block_with_label_sheet2(df_cat1_le_7_rc, "SRs Less Than or Equal to 7 Days (RC)", green_color_props, current_row_list_rc, worksheet, df_by_repair_closer.columns, content_start_row_rc_sheet)
    write_category_block_with_label_sheet2(df_cat2_gt7_le15_rc, "SRs More Than 7 But Less Than or Equal to 15 Days (RC)", yellow_color_props, current_row_list_rc, worksheet, df_by_repair_closer.columns, content_start_row_rc_sheet)
    write_category_block_with_label_sheet2(df_cat3_gt_15_rc, "SRs More Than 15 Days (RC)", red_color_props, current_row_list_rc, worksheet, df_by_repair_closer.columns, content_start_row_rc_sheet)

    return df_by_repair_closer # Return the processed DataFrame for the next sheet

def _tf3_export_sr_count_sheet(workbook, df_base_for_sr_count):
    """
    Exports a sheet named 'sr count' with unique SR numbers and all other columns,
    categorized by 'Days Passed' and formatted. This sheet uses data derived
    from the 'By Repair Closer' sheet.
    """
    print("DEBUG: Entering _tf3_export_sr_count_sheet function.")

    sr_no_col = clean_column_name("SR no.")
    
    # Ensure 'SR no.' column exists in the base DataFrame
    if sr_no_col not in df_base_for_sr_count.columns:
        print(f"WARNING: Column '{sr_no_col}' not found in the base data for 'sr count' sheet. Skipping this sheet.")
        return False

    # Drop duplicates based on 'SR no.' from the df_base_for_sr_count
    df_sr_count = df_base_for_sr_count.drop_duplicates(subset=[sr_no_col]).copy()
    
    # Get total record count for this sheet
    total_records_src = len(df_sr_count)
    
    if df_sr_count.empty:
        print("DEBUG: No unique SRs found for 'sr count' sheet after deduplication.")
        return True # Successfully processed, just nothing to write

    # This check needs to be conditional as well, if df_base_for_sr_count can be empty.
    # However, df_base_for_sr_count being empty is handled above.
    # The 'Days Passed' and 'Days Passed Category' columns *should* exist here if df_base_for_sr_count is not empty,
    # because they are calculated in _tf3_export_unclosed_repair_lines_by_repair_closer.
    # If they are not present, it implies a prior logic error or unexpected data.
    # For robust handling, we can explicitly create them if they somehow go missing, or handle the error gracefully.
    # For now, let's assume they *will* be there if df_base_for_sr_count is not empty.
    
    # Define the desired column order for the 'sr count' sheet.
    ordered_cols_for_sr_count = [
        clean_column_name("Branch name"),
        sr_no_col,
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
        'Ageing (Calculated)',
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
    ]
    # Filter to only include columns actually present in the DataFrame.
    final_export_columns_sr_count = [col for col in ordered_cols_for_sr_count if col in df_sr_count.columns]
    df_sr_count = df_sr_count.reindex(columns=final_export_columns_sr_count)


    worksheet = workbook.add_worksheet('sr count')

    # Define cell format properties (dictionaries)
    header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'border': 1, 'align': 'center'})
    
    # Base format properties (dictionaries)
    date_format_props_base = {'num_format': 'dd-mmm-yyyy'}
    integer_format_props_base = {'num_format': '#,##0'}
    text_format_props_base = {'text_wrap': False}
    
    # Create actual base format objects for column setting
    date_format = workbook.add_format(date_format_props_base)
    integer_format = workbook.add_format(integer_format_props_base)
    text_format = workbook.add_format(text_format_props_base)

    # Define color properties (dictionaries)
    green_color_props = {'bg_color': '#C6EFCE', 'font_color': '#006100'}
    yellow_color_props = {'bg_color': '#FFEB9C', 'font_color': '#9C6500'}
    red_color_props = {'bg_color': '#FFC7CE', 'font_color': '#9C0006'}

    category_label_format = workbook.add_format({
        'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter',
        'bg_color': '#ADD8E6', 'font_color': '#000000'
    })

    total_count_format = workbook.add_format({'bold': True, 'font_size': 11})

    current_row_sr_count = 0
    worksheet.write(current_row_sr_count, 0, f"Total Unique SRs: {total_records_src}", total_count_format)
    current_row_sr_count += 2

    # Write main column headers for this sheet
    for col_num, col_name in enumerate(df_sr_count.columns): # Use df_sr_count's actual columns
        worksheet.write(current_row_sr_count, col_num, col_name, header_format)
    current_row_sr_count += 1

    # Set column widths
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
    for i, col_name in enumerate(df_sr_count.columns): # Use df_sr_count's actual columns
        current_col_name_cleaned = clean_column_name(col_name)
        if current_col_name_cleaned in date_cols_to_format_local:
            worksheet.set_column(i, i, 15, date_format)
        elif 'description' in current_col_name_cleaned.lower() or \
             'summary' in current_col_name_cleaned.lower() or \
             'problem' in current_col_name_cleaned.lower() or \
             'investigation' in current_col_name_cleaned.lower():
            worksheet.set_column(i, i, 40, text_format)
        elif current_col_name_cleaned in ['Days Passed', 'con date'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
            worksheet.set_column(i, i, 15, integer_format)
        elif pd.api.types.is_numeric_dtype(df_sr_count[col_name]):
            worksheet.set_column(i, i, 12, integer_format)
        else:
            worksheet.set_column(i, i, 15)

    # Helper to write a block of categorized data including the new label row for this sheet
    def write_category_block_with_label_sr_count(df_category, label_text, color_properties, current_row_ref, worksheet_obj, final_cols, content_start_row):
        if not df_category.empty:
            if current_row_ref[0] > (content_start_row):
                current_row_ref[0] += 1
            
            worksheet_obj.merge_range(current_row_ref[0], 0, current_row_ref[0], len(final_cols) - 1,
                                     label_text, category_label_format)
            current_row_ref[0] += 1

            for _, row_data in df_category.iterrows():
                date_format_colored = workbook.add_format({**date_format_props_base, **color_properties})
                integer_format_colored = workbook.add_format({**integer_format_props_base, **color_properties})
                text_format_colored = workbook.add_format({**text_format_props_base, **color_properties})

                for col_num, col_name in enumerate(final_cols):
                    value = row_data.get(col_name)
                    current_col_name_cleaned = clean_column_name(col_name)

                    if current_col_name_cleaned in date_cols_to_format_local:
                        cell_format = date_format_colored
                    elif current_col_name_cleaned in ['Days Passed', 'con date'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
                        cell_format = integer_format_colored
                    elif pd.api.types.is_numeric_dtype(type(value)):
                        cell_format = integer_format_colored
                    else:
                        cell_format = text_format_colored

                    if pd.isna(value):
                        worksheet_obj.write_blank(current_row_ref[0], col_num, '', cell_format)
                    elif isinstance(value, (datetime, pd.Timestamp)):
                        worksheet_obj.write_datetime(current_row_ref[0], col_num, value, cell_format)
                    elif pd.api.types.is_numeric_dtype(type(value)):
                        if float(value) == int(value): 
                            worksheet_obj.write_number(current_row_ref[0], col_num, int(value), cell_format)
                        else: 
                            worksheet_obj.write_number(current_row_ref[0], col_num, value, cell_format)
                    else: 
                        worksheet_obj.write_string(current_row_ref[0], col_num, str(value), cell_format)
                current_row_ref[0] += 1
        return current_row_ref[0]

    # Categorize data for 'sr count' sheet
    df_cat1_le_7_src = df_sr_count[df_sr_count['Days Passed Category'] == "less than or equal to 7 days"].sort_values(by='Days Passed', ascending=True)
    df_cat2_gt7_le15_src = df_sr_count[df_sr_count['Days Passed Category'] == "more than 7 but less than or equal to 15 days"].sort_values(by='Days Passed', ascending=True)
    df_cat3_gt_15_src = df_sr_count[df_sr_count['Days Passed Category'] == "more than 15 days"].sort_values(by='Days Passed', ascending=True)

    current_row_list_src = [current_row_sr_count]
    content_start_row_sr_count_sheet = current_row_sr_count

    # Write category blocks for "sr count" sheet
    write_category_block_with_label_sr_count(df_cat1_le_7_src, "SRs Less Than or Equal to 7 Days", green_color_props, current_row_list_src, worksheet, df_sr_count.columns, content_start_row_sr_count_sheet)
    write_category_block_with_label_sr_count(df_cat2_gt7_le15_src, "SRs More Than 7 But Less Than or Equal to 15 Days", yellow_color_props, current_row_list_src, worksheet, df_sr_count.columns, content_start_row_sr_count_sheet)
    write_category_block_with_label_sr_count(df_cat3_gt_15_src, "SRs More Than 15 Days", red_color_props, current_row_list_src, worksheet, df_sr_count.columns, content_start_row_sr_count_sheet)

    return True

def export_unclosed_repairs_report(df_original_data, file_path):
    """
    Main export function for Unclosed Repairs Report (multi-sheet Excel).
    This function should be called from ex1.py.
    """
    print("DEBUG: Entering export_unclosed_repairs_report function (tf3_refactored).")

    if df_original_data is None or df_original_data.empty:
        print("No data provided for 'Unclosed Repair Lines' export.")
        return False

    try:
        writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
        workbook = writer.book
        
        # --- Sheet 1: Unclosed Repairs (Main Logic) ---
        df_export_main = df_original_data.copy()

        cleaned_repair_complete_date_col = clean_column_name('Repair complete date') # Still used in other sheets/helpers
        cleaned_repair_closure_date_col = clean_column_name('Repair closure date') # Still used for purple category but not main filter
        cleaned_sr_closure_date_col = clean_column_name('SR closure date') # Still used for purple category
        cleaned_sr_creation_date_col = clean_column_name('SR creation date')
        cleaned_sr_status_col = clean_column_name('SR status')
        cleaned_inter_org_challan_rc_branch_date_col = clean_column_name("Inter-org challan date from Repair center to Branch")

        # Check for absolutely required columns for this main report logic
        # Removed cleaned_repair_complete_date_col from this check as it's no longer a main filter for THIS sheet
        if cleaned_repair_closure_date_col not in df_export_main.columns:
            print(f"Error: Required column '{cleaned_repair_closure_date_col}' not found for main 'Unclosed Repairs' sheet. Export aborted.")
            workbook.close()
            return False
        if cleaned_sr_creation_date_col not in df_export_main.columns:
            print(f"Error: Required column '{cleaned_sr_creation_date_col}' not found for main 'Unclosed Repairs' sheet. Export aborted.")
            workbook.close()
            return False
        if cleaned_sr_status_col not in df_export_main.columns:
            print(f"Error: Required column '{cleaned_sr_status_col}' not found for main 'Unclosed Repairs' sheet. Export aborted.")
            workbook.close()
            return False
        if cleaned_inter_org_challan_rc_branch_date_col not in df_export_main.columns:
            print(f"Error: Required column '{cleaned_inter_org_challan_rc_branch_date_col}' not found for main 'Unclosed Repairs' sheet. Export aborted.")
            workbook.close()
            return False

        # Ensure relevant date columns are datetime objects
        # Removed df_export_main[cleaned_repair_complete_date_col] as it's not used for calculations/filters here
        df_export_main[cleaned_repair_closure_date_col] = pd.to_datetime(df_export_main[cleaned_repair_closure_date_col], errors='coerce')
        df_export_main[cleaned_sr_creation_date_col] = pd.to_datetime(df_export_main[cleaned_sr_creation_date_col], errors='coerce')
        df_export_main[cleaned_inter_org_challan_rc_branch_date_col] = pd.to_datetime(df_export_main[cleaned_inter_org_challan_rc_branch_date_col], errors='coerce')
        
        # Note: cleaned_sr_closure_date_col is explicitly checked for existence when needed for purple category
        if cleaned_sr_closure_date_col in df_export_main.columns:
            df_export_main[cleaned_sr_closure_date_col] = pd.to_datetime(df_export_main[cleaned_sr_closure_date_col], errors='coerce')


        # Main filter for Unclosed Repairs sheet:
        # SR status is NOT 'Closed'
        # AND Inter-org challan date from Repair center to Branch is NOT NaN
        # Removed Repair complete date is NaN logic, and also removed its check for .notna()
        temp_df_main_filtered = df_export_main[
            (df_export_main[cleaned_sr_status_col].astype(str).str.lower() != 'closed') &
            df_export_main[cleaned_inter_org_challan_rc_branch_date_col].notna()
        ].copy()

        # Initialize df_unclosed_repairs and df_purple_category to empty DataFrames
        # This handles cases where temp_df_main_filtered might be empty
        df_unclosed_repairs = pd.DataFrame(columns=temp_df_main_filtered.columns)
        df_purple_category = pd.DataFrame(columns=temp_df_main_filtered.columns)


        if not temp_df_main_filtered.empty: # Process only if there's data after initial filtering
            today = pd.Timestamp.now()

            # Calculate 'Days Passed' since 'Inter-org challan date from Repair center to Branch' (inclusive day count: +1)
            # THIS IS THE KEY CHANGE FOR SHEET 1
            temp_df_main_filtered['Days Passed'] = np.where(
                temp_df_main_filtered[cleaned_inter_org_challan_rc_branch_date_col].notna(),
                (today - temp_df_main_filtered[cleaned_inter_org_challan_rc_branch_date_col]).dt.days + 1,
                np.nan
            )
            temp_df_main_filtered['Days Passed'] = temp_df_main_filtered['Days Passed'].apply(lambda x: max(0, x) if pd.notna(x) else np.nan)
            temp_df_main_filtered['Days Passed'] = temp_df_main_filtered['Days Passed'].astype('Int64')

            # Calculate 'con date' for this sheet: today's date - SR creation date (inclusive day count: +1)
            if cleaned_sr_creation_date_col in temp_df_main_filtered.columns:
                temp_df_main_filtered['con date'] = np.where(
                    temp_df_main_filtered[cleaned_sr_creation_date_col].notna(),
                    (today - temp_df_main_filtered[cleaned_sr_creation_date_col]).dt.days + 1,
                    np.nan
                )
                temp_df_main_filtered['con date'] = temp_df_main_filtered['con date'].apply(lambda x: max(0, x) if pd.notna(x) else np.nan)
                temp_df_main_filtered['con date'] = temp_df_main_filtered['con date'].astype('Int64')
            else:
                temp_df_main_filtered['con date'] = np.nan
                print("Warning: 'SR creation date' missing, 'con date' for main report will be NaN.")

            def categorize_days_passed_main_report(days):
                if pd.isna(days): return ""
                elif days <= 7: return "less than or equal to 7 days"
                elif 7 < days <= 15: return "more than 7 but less than or equal to 15 days"
                else: return "more than 15 days"
                
            temp_df_main_filtered['Days Passed Category'] = temp_df_main_filtered['Days Passed'].apply(categorize_days_passed_main_report)
            
            # --- SPLIT PURPLE CATEGORY ---
            # Purple: Records that meet main filter criteria, BUT ALSO have a non-blank SR closure date.
            if cleaned_sr_closure_date_col in temp_df_main_filtered.columns:
                purple_mask = temp_df_main_filtered[cleaned_sr_closure_date_col].notna()
                df_purple_category = temp_df_main_filtered[purple_mask].copy()
                df_unclosed_repairs = temp_df_main_filtered[~purple_mask].copy()
            else:
                df_unclosed_repairs = temp_df_main_filtered.copy()
        
        # --- CREATE FINAL CATEGORY DATAFRAMES FOR WRITING ---
        df_cat1_le_7 = df_unclosed_repairs[df_unclosed_repairs['Days Passed Category'] == "less than or equal to 7 days"].sort_values(by='Days Passed', ascending=True)
        df_cat2_gt7_le15 = df_unclosed_repairs[df_unclosed_repairs['Days Passed Category'] == "more than 7 but less than or equal to 15 days"].sort_values(by='Days Passed', ascending=True)
        df_cat3_gt_15 = df_unclosed_repairs[df_unclosed_repairs['Days Passed Category'] == "more than 15 days"].sort_values(by='Days Passed', ascending=True)

        # --- CORRECTED COUNTING LOGIC ---
        total_unclosed_records = len(df_cat1_le_7) + len(df_cat2_gt7_le15) + len(df_cat3_gt_15)
        total_purple_records = len(df_purple_category)

        # --- Define Column Order ---
        ordered_cols_for_main_export = [
            clean_column_name("Branch name"),
            clean_column_name("SR no."),
            clean_column_name("SR creation date"),
            "con date", # Explicitly Column D
        ]
        # Collect all unique columns that are present in any of the dataframes to be exported to main sheet
        all_cols_in_dataframes = pd.Index([])
        if not df_cat1_le_7.empty: all_cols_in_dataframes = all_cols_in_dataframes.union(df_cat1_le_7.columns)
        if not df_cat2_gt7_le15.empty: all_cols_in_dataframes = all_cols_in_dataframes.union(df_cat2_gt7_le15.columns)
        if not df_cat3_gt_15.empty: all_cols_in_dataframes = all_cols_in_dataframes.union(df_cat3_gt_15.columns)
        if not df_purple_category.empty: all_cols_in_dataframes = all_cols_in_dataframes.union(df_purple_category.columns)
        
        # Ensure 'Days Passed' and 'Days Passed Category' are in the list if they were created
        if 'Days Passed' in temp_df_main_filtered.columns:
            all_cols_in_dataframes = all_cols_in_dataframes.union(pd.Index(['Days Passed']))
        if 'Days Passed Category' in temp_df_main_filtered.columns:
            all_cols_in_dataframes = all_cols_in_dataframes.union(pd.Index(['Days Passed Category']))


        all_other_cols_not_explicitly_ordered = [col for col in all_cols_in_dataframes if col not in ordered_cols_for_main_export and col not in ["Days Passed", "Days Passed Category"]]
        
        priority_cols_main = [
            clean_column_name("Incident Date"), clean_column_name("SR status"),
            clean_column_name("SR Problem type"), clean_column_name("Repair No"),
            clean_column_name("Meter Sr. No."), clean_column_name("Duration at Repair center"),
            clean_column_name("Item description"), clean_column_name("Product family"),
            clean_column_name("GRN date"),
            clean_column_name("Inter-org challan date from Branch to Repair center"),
            clean_column_name("Inter-org challan date from Repair center to Branch"), # Still in display
            clean_column_name("Sales Shipment Date"),
            'Ageing (Calculated)',
            clean_column_name("Defect in Lot(Repair line)"),
            clean_column_name("Problem Description"), clean_column_name("Problem Investigation"),
            clean_column_name("Diagnose code 1-Category"), clean_column_name("Diagnose code 1-Description"),
            clean_column_name("Diagnose code 2-Category"), clean_column_name("Diagnose code 2-Description"),
            clean_column_name("Diagnose code 3-Category"), clean_column_name("Diagnose code 3-Description"),
            clean_column_name("Service code"),
            clean_column_name("Repair complete date"), # Still in display
            clean_column_name("Repair closure date"),
            clean_column_name("Customer name"), clean_column_name("SR summary"),
            clean_column_name("Warranty status"), clean_column_name("SR closure date"), # Keep this for display if it exists
        ]
        for col_name in priority_cols_main:
            if col_name not in ordered_cols_for_main_export and col_name in all_other_cols_not_explicitly_ordered:
                ordered_cols_for_main_export.append(col_name)
                all_other_cols_not_explicitly_ordered.remove(col_name)

        final_export_columns_main = ordered_cols_for_main_export + sorted(list(set(all_other_cols_not_explicitly_ordered)))
        
        # Ensure Days Passed and Days Passed Category are at the end if they are present in the data.
        if 'Days Passed' in all_cols_in_dataframes and 'Days Passed' not in final_export_columns_main:
            final_export_columns_main.append('Days Passed')
        if 'Days Passed Category' in all_cols_in_dataframes and 'Days Passed Category' not in final_export_columns_main:
            final_export_columns_main.append('Days Passed Category')

        # Reindex all category DataFrames to the same final column order
        df_cat1_le_7 = df_cat1_le_7.reindex(columns=[col for col in final_export_columns_main if col in df_cat1_le_7.columns])
        df_cat2_gt7_le15 = df_cat2_gt7_le15.reindex(columns=[col for col in final_export_columns_main if col in df_cat2_gt7_le15.columns])
        df_cat3_gt_15 = df_cat3_gt_15.reindex(columns=[col for col in final_export_columns_main if col in df_cat3_gt_15.columns])
        df_purple_category = df_purple_category.reindex(columns=[col for col in final_export_columns_main if col in df_purple_category.columns])


        worksheet_main = workbook.add_worksheet('Unclosed Repairs')
        
        # --- Formats for the main sheet ---
        header_format = workbook.add_format({'bold': True, 'text_wrap': False, 'valign': 'top', 'border': 1, 'align': 'center'})
        
        date_format_props_base = {'num_format': 'dd-mmm-yyyy'}
        integer_format_props_base = {'num_format': '#,##0'}
        text_format_props_base = {'text_wrap': False}
        
        date_format = workbook.add_format(date_format_props_base)
        integer_format = workbook.add_format(integer_format_props_base)
        text_format = workbook.add_format(text_format_props_base)

        green_color_props = {'bg_color': '#C6EFCE', 'font_color': '#006100'}
        yellow_color_props = {'bg_color': '#FFEB9C', 'font_color': '#9C6500'}
        red_color_props = {'bg_color': '#FFC7CE', 'font_color': '#9C0006'}
        purple_color_props = {'bg_color': '#E2CFF4', 'font_color': '#5B00B7'}

        category_label_format = workbook.add_format({
            'bold': True, 'font_size': 12, 'align': 'left', 'valign': 'vcenter',
            'bg_color': '#ADD8E6', 'font_color': '#000000'
        })
        
        total_count_format = workbook.add_format({'bold': True, 'font_size': 11})

        # --- Write Total Count and Headers ---
        current_row_main = 0
        worksheet_main.write(current_row_main, 0, f"Total Unclosed Records (excluding unusual SR Closure): {total_unclosed_records}", total_count_format)
        if not df_purple_category.empty:
             worksheet_main.write(current_row_main + 1, 0, f"Total SRs with Unclosed Repair but have an SR Closure Date: {total_purple_records}", total_count_format)
             current_row_main += 1

        current_row_main += 2

        for col_num, col_name in enumerate(final_export_columns_main):
            worksheet_main.write(current_row_main, col_num, col_name, header_format)
        current_row_main += 1

        date_cols_to_format_excel_writer = [ 
            clean_column_name("SR creation date"), clean_column_name("Incident Date"),
            clean_column_name("GRN date"), clean_column_name("Repair complete date"),
            clean_column_name("SR closure date"), clean_column_name("Inter-org challan date from Branch to Repair center"),
            clean_column_name("Inter-org challan date from Repair center to Branch"),
            clean_column_name("Sales Shipment Date"), clean_column_name("Repair closure date")
        ]
        for i, col_name in enumerate(final_export_columns_main):
            current_col_name_cleaned = clean_column_name(col_name)
            if current_col_name_cleaned in date_cols_to_format_excel_writer:
                worksheet_main.set_column(i, i, 15, date_format)
            elif 'description' in current_col_name_cleaned.lower() or \
                 'summary' in current_col_name_cleaned.lower() or \
                 'problem' in current_col_name_cleaned.lower() or \
                 'investigation' in current_col_name_cleaned.lower():
                worksheet_main.set_column(i, i, 40, text_format)
            elif current_col_name_cleaned in ['Days Passed', 'con date'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
                worksheet_main.set_column(i, i, 15, integer_format)
            elif col_name in df_unclosed_repairs.columns and pd.api.types.is_numeric_dtype(df_unclosed_repairs[col_name]): # Use df_unclosed_repairs for type check after it's populated
                worksheet_main.set_column(i, i, 12, integer_format)
            else:
                worksheet_main.set_column(i, i, 15)

        def write_category_block_with_label(df_category, label_text, color_properties, current_row_ref, worksheet_obj, final_cols_order, content_start_row_for_spacing):
            if not df_category.empty:
                if current_row_ref[0] > (content_start_row_for_spacing):
                    current_row_ref[0] += 1
                
                worksheet_obj.merge_range(current_row_ref[0], 0, current_row_ref[0], len(final_cols_order) - 1,
                                         label_text, category_label_format)
                current_row_ref[0] += 1

                for _, row_data in df_category.iterrows():
                    date_format_colored = workbook.add_format({**date_format_props_base, **color_properties})
                    integer_format_colored = workbook.add_format({**integer_format_props_base, **color_properties})
                    text_format_colored = workbook.add_format({**text_format_props_base, **color_properties})

                    for col_num, col_name in enumerate(final_cols_order):
                        value = row_data.get(col_name)
                        current_col_name_cleaned = clean_column_name(col_name)

                        if current_col_name_cleaned in date_cols_to_format_excel_writer:
                            cell_format = date_format_colored
                        elif current_col_name_cleaned in ['Days Passed', 'con date'] or 'ageing' in current_col_name_cleaned.lower() or 'duration' in current_col_name_cleaned.lower():
                            cell_format = integer_format_colored
                        elif pd.api.types.is_numeric_dtype(type(value)):
                            cell_format = integer_format_colored
                        else:
                            cell_format = text_format_colored

                        if pd.isna(value):
                            worksheet_obj.write_blank(current_row_ref[0], col_num, '', cell_format)
                        elif isinstance(value, (datetime, pd.Timestamp)):
                            worksheet_obj.write_datetime(current_row_ref[0], col_num, value, cell_format)
                        elif pd.api.types.is_numeric_dtype(type(value)):
                            if float(value) == int(value): 
                                worksheet_obj.write_number(current_row_ref[0], col_num, int(value), cell_format)
                            else: 
                                worksheet_obj.write_number(current_row_ref[0], col_num, value, cell_format)
                        else: 
                            worksheet_obj.write_string(current_row_ref[0], col_num, str(value), cell_format)
                    current_row_ref[0] += 1
            return current_row_ref[0]

        current_row_list_main = [current_row_main]
        content_start_row_main_sheet = current_row_main
        write_category_block_with_label(df_cat1_le_7, "SRs Less Than or Equal to 7 Days", green_color_props, current_row_list_main, worksheet_main, final_export_columns_main, content_start_row_main_sheet)
        write_category_block_with_label(df_cat2_gt7_le15, "SRs More Than 7 But Less Than or Equal to 15 Days", yellow_color_props, current_row_list_main, worksheet_main, final_export_columns_main, content_start_row_main_sheet)
        write_category_block_with_label(df_cat3_gt_15, "SRs More Than 15 Days", red_color_props, current_row_list_main, worksheet_main, final_export_columns_main, content_start_row_main_sheet)
        if not df_purple_category.empty:
            write_category_block_with_label(df_purple_category, "SRs with Unclosed Repair but have an SR Closure Date (Unusual State)", purple_color_props, current_row_list_main, worksheet_main, final_export_columns_main, content_start_row_main_sheet)

        # --- Sheet 2: By Repair Closer ---
        df_for_sr_count_sheet = _tf3_export_unclosed_repair_lines_by_repair_closer(df_original_data, workbook)

        # --- Sheet 3: SR Count ---
        if df_for_sr_count_sheet is not None and not df_for_sr_count_sheet.empty:
            _tf3_export_sr_count_sheet(workbook, df_for_sr_count_sheet)
            print("DEBUG: 'sr count' sheet exported successfully.")
        elif df_for_sr_count_sheet is not None and df_for_sr_count_sheet.empty:
             print("DEBUG: 'By Repair Closer' sheet was empty, so 'sr count' sheet will also be empty.")
        else:
            print("WARNING: 'By Repair Closer' sheet creation failed or returned None. Cannot create 'sr count' sheet.")

        workbook.close()
        print(f"Successfully exported unclosed meter repair lines report to: {file_path}")
        return True
    except Exception as e:
        traceback.print_exc()
        print(f"An unexpected error occurred during export: {e}")
        return False

# For direct testing of the module:
if __name__ == '__main__':
    # Create a dummy DataFrame for testing tf3_refactored directly
    dummy_data = {
        "Branch name": ["B1", "B2", "B1", "B3", "B2", "B1", "B4", "B5", "B6", "B7", "B8"], # Added B8 for a new case
        "SR no.": ["SR001", "SR002", "SR003", "SR004", "SR005", "SR006", "SR007", "SR008", "SR009", "SR010", "SR011"], # Added SR011
        "SR creation date": [datetime(2025, 1, 1), datetime(2025, 1, 5), datetime(2025, 1, 10), datetime(2025, 2, 1), datetime(2025, 2, 5), datetime(2025,2,10), datetime(2025, 3, 1), datetime(2025, 3, 5), datetime(2025, 3, 15), datetime(2025, 3, 20), datetime(2025, 3, 25)],
        "Repair complete date": [datetime(2025, 1, 7), datetime(2025, 1, 18), datetime(2025, 1, 12), datetime(2025, 2, 3), pd.NaT, datetime(2025,2,25), pd.NaT, datetime(2025,3,10), datetime(2025,3,18), datetime(2025,3,22), datetime(2025,3,28)],
        "Repair closure date": [pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, datetime(2025,2,28), pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT],
        "SR closure date": [datetime(2025, 1, 10), pd.NaT, datetime(2025, 1, 15), pd.NaT, pd.NaT, datetime(2025,2,28), pd.NaT, pd.NaT, datetime(2025,3,20), pd.NaT, datetime(2025,3,29)], # SR009 has SR Closure Date, SR011 also
        "SR Problem type": ["Hardware", "Software", "Hardware", "Network", "Hardware", "Hardware", "Hardware", "Software", "Hardware", "Software", "Hardware"],
        "Meter Sr. No.": ["M001", "M002", "M003", "M004", "M005", "M006", "M007", "M008", "M009", "M010", "M011"],
        "GRN date": [datetime(2025,1,3), datetime(2025,1,7), datetime(2025,1,11), datetime(2025,2,2), pd.NaT, datetime(2025,2,12), datetime(2025,3,3), datetime(2025,3,7), datetime(2025,3,16), datetime(2025,3,21), datetime(2025,3,26)],
        "SR status": ["Open", "Open", "Open", "Open", "Open", "Closed", "Open", "Open", "Open", "Open", "Open"], # SR006 is Closed. SR009 & SR011 are Open with SR Closure Date (unusual).
        "Sales Shipment Date": [datetime(2024,12,1), datetime(2024,12,5), datetime(2024,12,10), datetime(2024,12,15), datetime(2024,12,20), datetime(2025,1,1), datetime(2025,2,1), datetime(2025,2,5), datetime(2025,3,1), datetime(2025,3,5), datetime(2025,3,10)],
        "Item description": ["Desc1", "Desc2", "Desc3", "Desc4", "Desc5", "Desc6", "Desc7", "Desc8", "Desc9", "Desc10", "Desc11"],
        "Defect in Lot(Repair line)": ["No", "Yes", "No", "No", "Yes", "No", "No", "Yes", "No", "Yes", "No"],
        "Problem Description": ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8", "P9", "P10", "P11"],
        "Problem Investigation": ["I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8", "I9", "I10", "I11"],
        "Diagnose code 1-Category": ["CatA", "CatB", "CatA", "CatA", "CatB", "CatC", "CatA", "CatD", "CatE", "CatF", "CatA"],
        "Diagnose code 1-Description": ["D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "D10", "D11"],
        "Diagnose code 2-Category": ["CatX", pd.NaT, "CatX", pd.NaT, "CatZ", pd.NaT, pd.NaT, "CatY", "CatX", pd.NaT, "CatZ"],
        "Diagnose code 2-Description": ["DX1", "", "DX1", "", "DZ1", "", "", "DY1", "DX2", "", "DZ2"],
        "Diagnose code 3-Category": [pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT],
        "Diagnose code 3-Description": [pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT, pd.NaT],
        "Repair No": [1, 2, 3, 4, pd.NA, 6, 7, 8, 9, 10, 11],
        "Duration at Repair center": [5, 10, 2, 3, pd.NA, 10, pd.NA, 5, 3, 2, 4],
        "Product family": ["Saral 100", "Prodigy", "Saral 100", "Apex", "Freedoms", "Elite 440", "Apex", "Prodigy", "Saral 100", "Apex", "Freedoms"],
        "Service code": ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10", "S11"],
        "Customer name": ["C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10", "C11"],
        "SR summary": ["Sum1", "Sum2", "Sum3", "Sum4", "Sum5", "Sum6", "Sum7", "Sum8", "Sum9", "Sum10", "Sum11"],
        "Warranty status": ["In Warranty", "Out of Warranty", "In Warranty", "In Warranty", "Out of Warranty", "In Warranty", "Out of Warranty", "In Warranty", "In Warranty", "Out of Warranty", "In Warranty"],
        "Inter-org challan date from Branch to Repair center": [datetime(2025,1,2), datetime(2025,1,6), datetime(2025,1,11), datetime(2025,2,2), pd.NaT, datetime(2025,2,11), datetime(2025,3,2), datetime(2025,3,6), datetime(2025,3,16), datetime(2025,3,21), datetime(2025,3,26)],
        "Inter-org challan date from Repair center to Branch": [datetime(2025,1,5), datetime(2025,1,15), datetime(2025,1,14), datetime(2025,2,5), pd.NaT, datetime(2025,2,27), pd.NaT, pd.NaT, datetime(2025,3,19), datetime(2025,3,23), datetime(2025,3,29)]
    }
    dummy_df = pd.DataFrame(dummy_data)

    print("Running tf3_refactored.py directly for testing...")
    output_path = "test_unclosed_repairs_report_final_revised_logic_with_interchallan_date.xlsx"
    success = export_unclosed_repairs_report(dummy_df, output_path)
    if success:
        print(f"Test report saved to {output_path}")
    else:
        print("Test report generation failed.")