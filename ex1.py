import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
import webbrowser
import os
from datetime import datetime, timedelta
import threading
import time
import multiprocessing
from queue import Empty # Import Empty from the standard queue library for the exception
from tkcalendar import DateEntry
import sys
# from PIL import Image, ImageTk # Not directly used in ex1.py for background image anymore, WelcomeScreen uses it.
import numpy as np
import dash1
from welcome_screen import WelcomeScreen

import customtkinter as ctk

from data_cleaning_utils import clean_column_name

from tf1_refactored import export_unreceived_meters_report
from tf2_refactored import export_unrepaired_meters_report
from tf3_refactored import export_unclosed_repairs_report
from tf4_refactored import export_received_not_repaired_report


# Global variables to manage the Dash server status
dash_thread = None
server_is_running = False
manager = None # Initialize manager globally for proper shutdown
# --- NEW: Global Queue for click data ---
click_data_queue = None

# Define common date columns for Tkinter to handle
TKINTER_DATE_COLUMNS = [
    "SR creation date",
    "Incident Date",
    "GRN date",
    "Repair complete date",
    "SR closure date",
    "Ageing as on today from sales shipment",
    "Sales Shipment Date",
    "Inter-org challan date from Branch to Repair center",
    "Inter-org challan date from Repair center to Branch",
    "Repair closure date"
]

# --- Product Family Categorization Logic ---
def map_product_family(product_family_value):
    if pd.isna(product_family_value):
        return pd.NA
    pf_lower = str(product_family_value).strip().lower()

    exact_matches = {
        "dpm's": "Panel Meter",
        "elite 440": "Panel Meter",
        "mfm (elite 500)": "Panel Meter",
        "transducer 1p": "Panel Meter",
        "transducer 3p": "Panel Meter",

        "proq 100": "Grid Meter",
        "summator 100": "Grid Meter",

        "saral 100": "SARAL Series",
        "saral 300": "SARAL Series",
        "saral 305 +": "SARAL Series",
        "saral 305": "SARAL Series",
        "skyline t45": "SARAL Series",

        "prodigy": "Credit Meter",

        "pi/pir": "Panel Instrument",

        "accuceck 1p": "Software & Special Product",
        "accuceck 3p": "Software & Special Product",
        "dcu": "Software & Special Product",
        "mri": "Software & Special Product",
        "pip it 500": "Software & Special Product",
        "m-cube 100": "Software & Special Product",
        "e-watch": "Software & Special Product",
        "cewe prometer": "Software & Special Product",
        "liberty online": "Software & Special Product",
        "skyline e90, 410": "Software & Special Product",
        "freedom ble": "Software & Special Product",
        "software": "Software & Special Product",

        "freedoms": "Control Products",
        "simple control": "Control Products",
        "service plus": "Control Products",
        "valve": "Control Products",
        "liberty connect 100": "Control Products",
        "thermosta t 600": "Control Products",

        "ltct": "Current Transformers",
        "mct": "Current Transformers",
    }

    if pf_lower in exact_matches:
        return exact_matches[pf_lower]

    contains_rules = [
        ("modem ecd", "Software & Special Product"),
        ("modem", "Software & Special Product"),

        ("apex", "Grid Meter"),
        ("prometer", "Grid Meter"),

        ("i-credit", "Credit Meter"),
        ("premier", "Credit Meter"),
        ("sprint", "Credit Meter"),

        ("gas meter", "Prepayment Meter"),
        ("liberty", "Prepayment Meter"),

        ("analog meters", "Panel Instrument"),

        ("beanbag", "Control Products"),

        ("temperature probe", "Pump Metering"),
        ("censeo", "Pump Metering"),
    ]

    for keyword, category in contains_rules:
        if keyword in pf_lower:
            return category

    return "Other / Uncategorized"

# --- HARDCODED METER GROUP LIST ---
# This list is derived from the categories returned by map_product_family
METER_GROUPS = [
    "Panel Meter",
    "Grid Meter",
    "SARAL Series",
    "Credit Meter",
    "Panel Instrument",
    "Software & Special Product",
    "Control Products",
    "Current Transformers",
    "Prepayment Meter",
    "Pump Metering",
    "Other / Uncategorized"
]
# --- END HARDCODED METER GROUP LIST ---


# --- Helper functions (defined locally in ex1.py as they process ex1.py's DataFrames) ---

def get_empty_dataframe_structure_f1():
    df = pd.DataFrame(columns=[
        'Branch name', 'SR no.', 'SR creation date', 'SR status', 'SR Problem type',
        'Product family', 'Product Family Group', # Product Family Group is derived
        'Diagnose code 1-Category', 'Diagnose code 2-Category',
        'Diagnose code 3-Category', 'GRN date', 'Inter-org challan date from Repair center to Branch',
        'SR closure date',
        'Ageing', 'Ageing Category',
        'M1_TAT', 'M2_TAT', 'M3_TAT',
        'M1_TAT_Category', 'M2_TAT_Category', 'M3_TAT_Category',
        'Overall_TAT', 'Overall_TAT_Category', 'MonthYear',
        'Simplified SR Status' # New column for simplified status
    ])
    for col in TKINTER_DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.Series(dtype='datetime64[ns]')

    schema_hints = {
        'Branch name': str, 'SR no.': str, 'SR status': str, 'SR Problem type': str,
        'Product family': str, 'Product Family Group': str,
        'Diagnose code 1-Category': str, 'Diagnose code 2-Category': str, 'Diagnose code 3-Category': str,
        'Ageing': 'Int64', 'Ageing Category': str,
        'M1_TAT': 'Int64', 'M2_TAT': 'Int64', 'M3_TAT': 'Int64',
        'M1_TAT_Category': str, 'M2_TAT_Category': str, 'M3_TAT_Category': str,
        'Overall_TAT': 'Int64', 'Overall_TAT_Category': str, 'MonthYear': str,
        'Simplified SR Status': str # New column for simplified status
    }

    for col, dtype in schema_hints.items():
        if col in df.columns:
            if dtype == str:
                df[col] = pd.Series(dtype='object')
            elif dtype == 'Int64':
                df[col] = pd.Series(dtype='Int64')

    return df

def ensure_datetime_columns_f1(df, columns_to_check=TKINTER_DATE_COLUMNS):
    if df is None or not isinstance(df, pd.DataFrame):
        return get_empty_dataframe_structure_f1()

    df_copy = df.copy()

    for col in columns_to_check:
        if col in df_copy.columns:
            if not pd.api.types.is_datetime64_any_dtype(df_copy[col]):
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce', infer_datetime_format=True)
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]) and df_copy[col].dt.tz is not None:
                df_copy[col] = df_copy[col].dt.tz_localize(None)

    if 'SR no.' in df_copy.columns:
        df_copy['SR no.'] = df_copy['SR no.'].astype(str)

    return df_copy

def calculate_m1_tat_f1(row):
    if pd.isna(row['SR creation date']): return pd.NA
    receiving_date = row.get('GRN date', pd.NaT)
    if pd.isna(receiving_date): receiving_date = pd.Timestamp.now().normalize()
    receiving_date = pd.to_datetime(receiving_date, errors='coerce').normalize() if pd.notna(receiving_date) else pd.NaT
    sr_creation_date = pd.to_datetime(row['SR creation date'], errors='coerce').normalize() if pd.notna(row['SR creation date']) else pd.NaT
    if pd.notna(receiving_date) and pd.notna(sr_creation_date):
        return (receiving_date - sr_creation_date).days
    return pd.NA

def calculate_m2_tat_f1(row):
    if pd.isna(row.get('GRN date', pd.NaT)): return pd.NA
    dispatch_date = row.get('Inter-org challan date from Repair center to Branch', pd.NaT)
    if pd.isna(dispatch_date): dispatch_date = pd.Timestamp.now().normalize()
    dispatch_date = pd.to_datetime(dispatch_date, errors='coerce').normalize() if pd.notna(dispatch_date) else pd.NaT
    meter_received_date = pd.to_datetime(row['GRN date'], errors='coerce').normalize() if pd.notna(row['GRN date']) else pd.NaT
    if pd.notna(dispatch_date) and pd.notna(meter_received_date):
        return (dispatch_date - meter_received_date).days
    return pd.NA

def calculate_m3_tat_f1(row):
    if pd.isna(row.get('Inter-org challan date from Repair center to Branch', pd.NaT)): return pd.NA
    sr_closure_date = row.get('SR closure date', pd.NaT)
    if pd.isna(sr_closure_date): sr_closure_date = pd.Timestamp.now().normalize()
    sr_closure_date = pd.to_datetime(sr_closure_date, errors='coerce').normalize() if pd.notna(sr_closure_date) else pd.NaT
    dispatch_date = pd.to_datetime(row['Inter-org challan date from Repair center to Branch'], errors='coerce').normalize() if pd.notna(row['Inter-org challan date from Repair center to Branch']) else pd.NaT
    if pd.notna(sr_closure_date) and pd.notna(dispatch_date):
        return (sr_closure_date - dispatch_date).days
    return pd.NA

def process_df_to_json_f1(df):
    if df is None or df.empty:
        df_copy = get_empty_dataframe_structure_f1()
    else:
        df_copy = df.copy()

    df_copy = ensure_datetime_columns_f1(df_copy, TKINTER_DATE_COLUMNS)

    nullable_int_cols = ['Ageing', 'M1_TAT', 'M2_TAT', 'M3_TAT', 'Overall_TAT', 'Repair No', 'Duration at Repair center']
    for col in nullable_int_cols:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').astype('Int64')

    if 'SR no.' in df_copy.columns:
        df_copy['SR no.'] = df_copy['SR no.'].astype(str)

    if 'Product family' in df_copy.columns:
        df_copy['Product Family Group'] = df_copy['Product family'].apply(map_product_family)
    else:
        if 'Product Family Group' not in df_copy.columns:
            df_copy['Product Family Group'] = "Other / Uncategorized"
        else:
            df_copy['Product Family Group'] = df_copy['Product Family Group'].fillna("Other / Uncategorized")

    if 'SR status' in df_copy.columns:
        df_copy['Simplified SR Status'] = df_copy['SR status'].astype(str).str.lower().apply(
            lambda x: 'closed' if x == 'closed' else 'open'
        ).str.capitalize()
    else:
        df_copy['Simplified SR Status'] = 'N/A'

    if all(col in df_copy.columns for col in ['SR creation date', 'SR closure date']):
        def calculate_ageing_with_closure_logic(row):
            creation_date = row['SR creation date']
            closure_date = row.get('SR closure date', pd.NaT)
            if pd.isna(creation_date): return pd.NA
            creation_date_norm = creation_date.normalize()
            if pd.notna(closure_date):
                closure_date_norm = closure_date.normalize()
                return (closure_date_norm - creation_date_norm).days
            else:
                today_norm = pd.Timestamp.now().normalize()
                return (today_norm - creation_date_norm).days

        df_copy['Ageing'] = df_copy.apply(calculate_ageing_with_closure_logic, axis=1).astype('Int64')

        ageing_bins = [0, 21, 30, 45, 90, float("inf")]
        ageing_labels = ["0-21 days", "22-30 days", "31-45 days", "46-90 days", "90+ days"]
        all_ageing_categories = ageing_labels + ['N/A']

        df_copy['Ageing Category'] = pd.cut(
            df_copy['Ageing'],
            bins=ageing_bins,
            labels=ageing_labels,
            right=False,
            include_lowest=True
        )
        df_copy['Ageing Category'] = df_copy['Ageing Category'].astype(
            pd.CategoricalDtype(categories=all_ageing_categories, ordered=True)
        )
        df_copy['Ageing Category'] = df_copy['Ageing Category'].fillna('N/A')
    else:
        if 'Ageing' not in df_copy.columns:
            df_copy['Ageing'] = pd.Series(dtype='Int64')
        if 'Ageing Category' not in df_copy.columns:
            df_copy['Ageing Category'] = pd.Series('N/A', index=df_copy.index, dtype=pd.CategoricalDtype(categories=["0-21 days", "22-30 days", "31-45 days", "46-90 days", "90+ days", 'N/A'], ordered=True))
        else:
            df_copy['Ageing Category'] = df_copy['Ageing Category'].fillna('N/A')

    required_tat_cols = ["SR creation date", "GRN date", "Inter-org challan date from Repair center to Branch", "SR closure date"]
    if all(col in df_copy.columns for col in required_tat_cols) and not df_copy.empty:
        df_copy['M1_TAT'] = df_copy.apply(calculate_m1_tat_f1, axis=1).astype('Int64')
        df_copy['M2_TAT'] = df_copy.apply(calculate_m2_tat_f1, axis=1).astype('Int64')
        df_copy['M3_TAT'] = df_copy.apply(calculate_m3_tat_f1, axis=1).astype('Int64')

        tat_bins_stage = [-float('inf'), 6, 10, 15, float('inf')]
        tat_labels_stage = ['<7D', '7-10D', '11-15D', '>15D']
        all_stage_tat_categories = tat_labels_stage + ['N/A']

        df_copy['M1_TAT_Category'] = pd.cut(df_copy['M1_TAT'], bins=tat_bins_stage, labels=tat_labels_stage, right=True, include_lowest=True)
        df_copy['M2_TAT_Category'] = pd.cut(df_copy['M2_TAT'], bins=tat_bins_stage, labels=tat_labels_stage, right=True, include_lowest=True)
        df_copy['M3_TAT_Category'] = pd.cut(df_copy['M3_TAT'], bins=tat_bins_stage, labels=tat_labels_stage, right=True, include_lowest=True)

        for col_name in ['M1_TAT_Category', 'M2_TAT_Category', 'M3_TAT_Category']:
            df_copy[col_name] = df_copy[col_name].astype(pd.CategoricalDtype(categories=all_stage_tat_categories, ordered=True))
            df_copy[col_name] = df_copy[col_name].fillna('N/A')
    else:
        all_stage_tat_categories = ['<7D', '7-10D', '11-15D', '>15D', 'N/A']
        for col_name in ['M1_TAT', 'M2_TAT', 'M3_TAT']:
            if col_name not in df_copy.columns:
                df_copy[col_name] = pd.Series(dtype='Int64')
        for col_name in ['M1_TAT_Category', 'M2_TAT_Category', 'M3_TAT_Category']:
            if col_name not in df_copy.columns:
                df_copy[col_name] = pd.Series('N/A', index=df_copy.index, dtype=pd.CategoricalDtype(categories=all_stage_tat_categories, ordered=True))
            else:
                df_copy[col_name] = df_copy[col_name].fillna('N/A')

    if all(col in df_copy.columns for col in ['SR creation date', 'SR closure date', 'SR status']):
        closed_mask = df_copy['Simplified SR Status'].astype(str).str.lower() == 'closed'
        creation_dates_normalized = df_copy['SR creation date'].dt.normalize()
        closure_dates_normalized = df_copy['SR closure date'].dt.normalize()

        df_copy['Overall_TAT'] = (closure_dates_normalized - creation_dates_normalized).dt.days
        df_copy.loc[~closed_mask | df_copy['Overall_TAT'].isna(), 'Overall_TAT'] = pd.NA
        df_copy['Overall_TAT'] = df_copy['Overall_TAT'].astype('Int64')

        tat_bins_overall = [-float('inf'), 21, 30, float('inf')]
        tat_labels_overall = ['<=21D', '21-30 D', '>30D']
        all_overall_tat_categories = tat_labels_overall + ['OPEN', 'N/A Status/Date']

        df_copy['Overall_TAT_Category'] = pd.cut(df_copy['Overall_TAT'], bins=tat_bins_overall, labels=tat_labels_overall, right=True, include_lowest=True)
        df_copy['Overall_TAT_Category'] = df_copy['Overall_TAT_Category'].astype(pd.CategoricalDtype(categories=all_overall_tat_categories, ordered=True))

        open_mask = df_copy['Simplified SR Status'].astype(str).str.lower() == 'open'
        df_copy.loc[open_mask, 'Overall_TAT_Category'] = 'OPEN'

        df_copy['Overall_TAT_Category'] = df_copy['Overall_TAT_Category'].fillna('N/A Status/Date')
    else:
        if 'Overall_TAT' not in df_copy.columns:
            df_copy['Overall_TAT'] = pd.Series(dtype='Int64')
        all_overall_tat_categories_empty = ['<=21D', '21-30 D', '>30D', 'OPEN', 'N/A Status/Date']
        if 'Overall_TAT_Category' not in df_copy.columns:
            df_copy['Overall_TAT_Category'] = pd.Series('N/A Status/Date', index=df_copy.index, dtype=pd.CategoricalDtype(categories=all_overall_tat_categories_empty, ordered=True))
        else:
            df_copy['Overall_TAT_Category'] = df_copy['Overall_TAT_Category'].fillna('N/A Status/Date')

    if 'SR creation date' in df_copy.columns:
        df_copy['MonthYear'] = df_copy['SR creation date'].dt.strftime('%b-%y')
    else:
        if 'MonthYear' not in df_copy.columns:
            df_copy['MonthYear'] = 'N/A Month'

    for col in df_copy.columns:
        if df_copy[col].dtype == 'object':
            if col not in ['Branch name', 'SR no.', 'SR status', 'SR Problem type', 'Product family',
                            'Product Family Group', 'Diagnose code 1-Category', 'Diagnose code 2-Category',
                            'Diagnose code 3-Category', 'Ageing Category', 'M1_TAT_Category', 'M2_TAT_Category',
                            'M3_TAT_Category', 'Overall_TAT_Category', 'MonthYear', 'Item description',
                            'Defect in Lot(Repair line)', 'Simplified SR Status',
                            'Problem Description', 'Problem Investigation',
                            'Customer name', 'SR summary', 'Warranty status', 'Meter Sr. No.']:
                try:
                    df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')
                    if pd.api.types.is_numeric_dtype(df_copy[col]) and not df_copy[col].isnull().all():
                        df_copy[col] = df_copy[col].astype('Int64')
                    else:
                        df_copy[col] = df_copy[col].apply(lambda x: None if pd.isna(x) else x)
                except Exception as e:
                    df_copy[col] = df_copy[col].apply(lambda x: None if pd.isna(x) else x)
            else:
                df_copy[col] = df_copy[col].apply(lambda x: None if pd.isna(x) else x)

    return df_copy.to_json(date_format='iso', orient='split')


class ExcelApp:
    def __init__(self, root, shared_data_proxy_instance, click_queue_instance):
        ctk.set_appearance_mode("Dark")
        ctk.set_default_color_theme("blue")

        self.root = root
        # We will set the state in the main execution block using .after()
        self.root.title("Application Loading...")

        self.style = ttk.Style()
        self.style.theme_use('clam')

        if getattr(sys, 'frozen', False):
            self.base_path = sys._MEIPASS
        else:
            self.base_path = os.path.abspath(".")

        self.welcome_screen = WelcomeScreen(self.root, self._on_file_selected_from_welcome, self.base_path)

        self.df_original = None
        self.df_filtered_for_table = None
        self.df_filtered_for_charts = None

        self.meter_group_filter_vars = {group: tk.BooleanVar(value=True) for group in METER_GROUPS}
        self.all_meter_groups_var = tk.BooleanVar(value=True)
        self._updating_meter_group_checkboxes_flag = False

        self.last_graph_clicked_sr_numbers = []
        self.graph_highlight_color = '#d2e3f5'
        self.graph_filter_active = False

        self.main_widgets_created = False
        self._create_main_dashboard_widgets()

        self._clear_date_entry(self.from_date_entry)
        self._clear_date_entry(self.to_date_entry)
        self.date_filter_mode_dropdown.set("All")
        self._toggle_date_filters()

        self._update_meter_group_display_text()


        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

        self.shared_data_proxy = shared_data_proxy_instance
        self.click_data_queue = click_queue_instance

        self.start_dash_server()
        self.check_for_dash_clicks()

    def _change_appearance_mode_event(self, new_appearance_mode: str):
        ctk.set_appearance_mode(new_appearance_mode)
        if new_appearance_mode == "Dark":
            self.from_date_entry.config(background='darkblue', foreground='white', normalforeground='white', headersbackground='grey', headersforeground='white')
            self.to_date_entry.config(background='darkblue', foreground='white', normalforeground='white', headersbackground='grey', headersforeground='white')
        else:
            self.from_date_entry.config(background='white', foreground='black', normalforeground='black', headersbackground='grey', headersforeground='black')
            self.to_date_entry.config(background='white', foreground='black', normalforeground='black', headersbackground='grey', headersforeground='black')


    def _create_main_dashboard_widgets(self):
        self.style.configure("Custom.Treeview",
            background="#FFFFFF",
            fieldbackground="#FFFFFF",
            foreground="#333333",
            font=("Inter", 9),
            rowheight=26,
            bordercolor="#c0c0c0",
            lightcolor="#c0c0c0",
            darkcolor="#c0c0c0",
            borderwidth=1,
            relief="solid",
            padding=[2, 2]
        )
        self.style.map('Custom.Treeview',
            background=[('selected', '#a8d2ff')],
            foreground=[('selected', '#000000')],
            fieldbackground=[('selected', '#a8d2ff')]
        )
        self.tree_tag_colors = {
            'oddrow': '#f8f8f8',
            'evenrow': '#ffffff'
        }
        for tag, color in self.tree_tag_colors.items():
            self.style.configure(tag, background=color)

        self.style.configure("highlight", background="#FFD700", foreground="black")
        self.style.configure("graph_highlight", background=self.graph_highlight_color, foreground="black")


        self.style.configure("Custom.Treeview.Heading",
            background="#e6e6e6",
            foreground="#374151",
            font=("Inter", 9, "bold"),
            padding=[5, 8],
            relief="raised",
            borderwidth=1,
            bordercolor="#909090"
        )
        self.style.map("Custom.Treeview.Heading",
            background=[('active', '#d0d0d0')]
        )

        self.style.configure("TScrollbar",
            gripcount=0,
            relief="flat",
            background="#2E86C1", # Darker blue
            lightcolor="#3498DB", # Medium blue
            darkcolor="#2874A6",  # Slightly darker blue
            bordercolor="#1F618D", # Dark blue border
            troughcolor="#A9CCE3" # Very light blue for the track
        )
        self.style.map("TScrollbar",
            background=[('active', '#3498DB')] # Active state blue
        )

        self.top_controls_frame = ctk.CTkFrame(self.root, fg_color=("gray90", "gray20"))
        self.top_controls_frame.grid_columnconfigure(0, weight=1)
        self.top_controls_frame.grid_columnconfigure(1, weight=0)
        self.top_controls_frame.grid_columnconfigure(2, weight=1)
        self.top_controls_frame.grid_columnconfigure(3, weight=0)
        self.top_controls_frame.grid_columnconfigure(4, weight=0)
        self.top_controls_frame.grid_columnconfigure(5, weight=0)


        self.open_excel_button_in_main = ctk.CTkButton(self.top_controls_frame, text="Open New Excel File", command=self.load_excel, width=200, height=40, font=("Inter", 11, "bold"), corner_radius=8)
        self.open_excel_button_in_main.grid(row=0, column=1, pady=0, padx=0, sticky="nsew")

        self.appearance_mode_label = ctk.CTkLabel(self.top_controls_frame, text="Appearance Mode:", font=("Inter", 10))
        self.appearance_mode_label.grid(row=0, column=3, padx=(20, 5), pady=0, sticky="e")
        self.appearance_mode_optionemenu = ctk.CTkOptionMenu(self.top_controls_frame, values=["Light", "Dark", "System"],
                                                             command=self._change_appearance_mode_event, width=100)
        self.appearance_mode_optionemenu.grid(row=0, column=4, padx=(0, 0), pady=0, sticky="w")
        self.appearance_mode_optionemenu.set(ctk.get_appearance_mode())

        self.frame_filters = ctk.CTkFrame(self.root, corner_radius=10, fg_color=("gray90", "gray20"), border_width=2, border_color=("gray70", "gray30"))

        ctk.CTkLabel(self.frame_filters, text="Data Filters (for Table View)", font=("Inter", 12, "bold")).grid(row=0, column=0, columnspan=7, sticky='w', padx=10, pady=(5,0))

        ctk.CTkLabel(self.frame_filters, text="SR Type:", font=("Inter", 10)).grid(row=1, column=0, sticky='w', padx=5, pady=2)
        ctk.CTkLabel(self.frame_filters, text="SR Status:", font=("Inter", 10)).grid(row=1, column=1, sticky='w', padx=5, pady=2)
        ctk.CTkLabel(self.frame_filters, text="Date Filter Mode:", font=("Inter", 10)).grid(row=1, column=2, sticky='w', padx=5, pady=2)
        ctk.CTkLabel(self.frame_filters, text="Date Range:", font=("Inter", 10)).grid(row=1, column=3, columnspan=2, sticky='w', padx=5, pady=2)
        ctk.CTkLabel(self.frame_filters, text="Meter Group:", font=("Inter", 10)).grid(row=1, column=5, sticky='w', padx=5, pady=2)

        self.sr_type_dropdown = ctk.CTkComboBox(self.frame_filters, values=["All"], state='readonly', width=180, font=("Inter", 10), command=self.on_sr_type_select)
        self.sr_type_dropdown.set("All")
        self.sr_type_dropdown.grid(row=2, column=0, padx=5, pady=2, sticky='ew')

        self.sr_status_dropdown = ctk.CTkComboBox(self.frame_filters, values=["All", "Open", "Closed"], state='readonly', width=180, font=("Inter", 10), command=self.on_sr_status_select)
        self.sr_status_dropdown.set("All")
        self.sr_status_dropdown.grid(row=2, column=1, padx=5, pady=2, sticky='ew')

        self.date_filter_mode_dropdown = ctk.CTkComboBox(self.frame_filters, values=["All", "Custom Date Range"], state='readonly', width=180, font=("Inter", 10), command=self.on_date_filter_mode_select)
        self.date_filter_mode_dropdown.set("All")
        self.date_filter_mode_dropdown.grid(row=2, column=2, padx=5, pady=2, sticky='ew')

        self.date_range_container = ctk.CTkFrame(self.frame_filters, fg_color="transparent")
        self.date_range_container.grid(row=2, column=3, columnspan=2, padx=5, pady=2, sticky='ew')

        self.from_date_entry = DateEntry(self.date_range_container, width=12, background='darkblue',
                                         foreground='white', borderwidth=2,
                                         date_pattern='dd-mm-yyyy',
                                         selectmode='day',
                                         normalforeground='white',
                                         selectedbackground='lightblue',
                                         selectedforeground='black',
                                         headersbackground='grey',
                                         headersforeground='white',
                                         state='readonly'
                                        )
        self.from_date_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0,0))
        self.from_date_entry.bind("<<DateSelected>>", self._on_date_entry_selected)

        ctk.CTkLabel(self.date_range_container, text=" → ", font=("Inter", 12, "bold")).pack(side=tk.LEFT, padx=2)

        self.to_date_entry = DateEntry(self.date_range_container, width=12, background='darkblue',
                                       foreground='white', borderwidth=2,
                                       date_pattern='dd-mm-yyyy',
                                       selectmode='day',
                                       normalforeground='white',
                                       selectedbackground='lightblue',
                                       selectedforeground='black',
                                       headersbackground='grey',
                                       headersforeground='white',
                                       state='readonly'
                                      )
        self.to_date_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0,0))
        self.to_date_entry.bind("<<DateSelected>>", self._on_date_entry_selected)

        self.meter_group_display_var = ctk.StringVar(value="All Selected")
        self.meter_group_display_entry = ctk.CTkEntry(self.frame_filters, textvariable=self.meter_group_display_var, state='readonly', width=180, font=("Inter", 10), corner_radius=6)
        self.meter_group_display_entry.grid(row=2, column=5, padx=5, pady=2, sticky='ew')

        self.open_meter_group_popup_button = ctk.CTkButton(self.frame_filters, text="...", command=self._show_meter_group_selection_popup, width=40, height=28, font=("Inter", 10, "bold"), corner_radius=6)
        self.open_meter_group_popup_button.grid(row=2, column=5, padx=(0,0), pady=2, sticky='e')

        self.apply_filters_button = ctk.CTkButton(self.frame_filters, text="Apply Table Filters", command=self.apply_filters, width=180, height=35, font=("Inter", 10, "bold"), corner_radius=8)
        self.apply_filters_button.grid(row=2, column=6, padx=10, pady=2, sticky='e')

        self.graph_filter_info_label = ctk.CTkLabel(self.frame_filters, text="", font=("Inter", 9, "italic"), text_color="blue")
        self.graph_filter_info_label.grid(row=3, column=0, columnspan=5, sticky='w', padx=5, pady=2)

        self.clear_graph_filter_button = ctk.CTkButton(self.frame_filters, text="Clear Graph Filter", command=self._clear_graph_filter, width=150, height=30, font=("Inter", 9, "bold"), corner_radius=8)
        self.clear_graph_filter_button.grid(row=3, column=6, padx=10, pady=2, sticky='e')
        self.clear_graph_filter_button.grid_remove()

        self.frame_filters.grid_columnconfigure(0, weight=1)
        self.frame_filters.grid_columnconfigure(1, weight=1)
        self.frame_filters.grid_columnconfigure(2, weight=1)
        self.frame_filters.grid_columnconfigure(3, weight=1)
        self.frame_filters.grid_columnconfigure(4, weight=1)
        self.frame_filters.grid_columnconfigure(5, weight=1)
        self.frame_filters.grid_columnconfigure(6, weight=0)

        self.frame_buttons = ctk.CTkFrame(self.root, fg_color=("gray90", "gray20"))

        self.show_dashboard_button = ctk.CTkButton(self.frame_buttons, text="Show Visual Dashboard", command=self.show_plotly_dashboard, width=200, height=35, font=("Inter", 10, "bold"), corner_radius=8)
        self.export_filtered_button = ctk.CTkButton(self.frame_buttons, text="Export Filtered Table Data", command=self.export_filtered_data, width=200, height=35, font=("Inter", 10, "bold"), corner_radius=8)
        self.unreceived_meters_button = ctk.CTkButton(self.frame_buttons, text="Unreceived Meter Data", command=self.export_unreceived_meters, width=200, height=35, font=("Inter", 10, "bold"), corner_radius=8)
        self.unrepaired_meters_button = ctk.CTkButton(self.frame_buttons, text="Unrepaired Meters", command=self.export_unrepaired_meters_report_main, width=200, height=35, font=("Inter", 10, "bold"), corner_radius=8)
        self.unclosed_repairs_button = ctk.CTkButton(self.frame_buttons, text="Unclosed Repair Lines", command=self.export_unclosed_repair_lines_main, width=200, height=35, font=("Inter", 10, "bold"), corner_radius=8)
        self.sr_category_button = ctk.CTkButton(self.frame_buttons, text="SR Category", command=self.export_sr_category_main, width=200, height=35, font=("Inter", 10, "bold"), corner_radius=8)
        
        for i in range(6):
            self.frame_buttons.grid_columnconfigure(i, weight=1)

        self.show_dashboard_button.grid(row=0, column=0, padx=8)
        self.export_filtered_button.grid(row=0, column=1, padx=8)
        self.unreceived_meters_button.grid(row=0, column=2, padx=8)
        self.unrepaired_meters_button.grid(row=0, column=3, padx=8)
        self.unclosed_repairs_button.grid(row=0, column=4, padx=8)
        self.sr_category_button.grid(row=0, column=5, padx=8)

        self.tree_frame = ctk.CTkFrame(self.root, corner_radius=10, fg_color=("gray90", "gray20"), border_width=2, border_color=("gray70", "gray30"))

        self.tree = ttk.Treeview(self.tree_frame, style="Custom.Treeview", show="headings tree")

        self.tree.heading("#0", text="S.No.", anchor='center')
        self.tree.column("#0", width=60, minwidth=40, anchor='center', stretch=False)

        self.tree.tag_configure('oddrow', background=self.tree_tag_colors['oddrow'])
        self.tree.tag_configure('evenrow', background=self.tree_tag_colors['evenrow'])
        self.tree.tag_configure('graph_highlight', background=self.graph_highlight_color, foreground="black")


        self.vsb = ttk.Scrollbar(self.tree_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=self.vsb.set)

        self.hsb = ttk.Scrollbar(self.tree_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(xscrollcommand=self.hsb.set)

        self.tree_frame.grid_rowconfigure(0, weight=1)
        self.tree_frame.grid_rowconfigure(1, weight=0)
        self.tree_frame.grid_columnconfigure(0, weight=1)
        self.tree_frame.grid_columnconfigure(1, weight=0)

        self.tree.grid(row=0, column=0, sticky='nsew', padx=5, pady=5)
        self.vsb.grid(row=0, column=1, sticky='ns')
        self.hsb.grid(row=1, column=0, sticky='ew', padx=5)

        self.editing_entry = None
        self.tree.bind('<Double-1>', self.start_editing_cell)

        self.main_widgets_created = True


    def _on_file_selected_from_welcome(self, file_path):
        if file_path:
            self.welcome_screen.destroy()
            self.welcome_screen = None

            self.show_main_dashboard_widgets()
            self.load_excel(file_path=file_path)

    def show_main_dashboard_widgets(self):
        if not self.main_widgets_created:
            print("ERROR: Main dashboard widgets not created yet.")
            return

        self.root.title("Excel Viewer and Exporter with Graphs")
        self.root.state('zoomed')


        self.top_controls_frame.pack(fill='x', padx=20, pady=(15,0))
        self.frame_filters.pack(pady=15, fill='x', padx=20)

        self.search_frame = ctk.CTkFrame(self.root, fg_color="transparent")
        self.search_frame.pack(pady=10, fill='x', padx=20)

        self.search_entry = ctk.CTkEntry(self.search_frame, font=("Inter", 11), placeholder_text="Search table data...", corner_radius=8)
        self.search_entry.bind("<FocusIn>", self.clear_search_placeholder)
        self.search_entry.bind("<FocusOut>", self.restore_search_placeholder)
        self.search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))

        self.search_button = ctk.CTkButton(self.search_frame, text="Search", command=self.filter_table_data, width=100, height=35, font=("Inter", 10, "bold"), corner_radius=8)
        self.search_button.pack(side=tk.RIGHT)

        self.frame_buttons.pack(pady=15)

        self.tree_frame.pack(fill=tk.BOTH, expand=True, padx=20, pady=15)

        self.search_entry.focus_set()

    def on_sr_type_select(self, choice):
        self.apply_filters()

    def on_sr_status_select(self, choice):
        self.apply_filters()

    def on_date_filter_mode_select(self, choice):
        self._toggle_date_filters()


    def _clear_date_entry(self, date_entry_widget):
        date_entry_widget.set_date(None)
        date_entry_widget.delete(0, tk.END)
        date_entry_widget.insert(0, "All")

    def _on_date_entry_selected(self, event):
        if self.date_filter_mode_dropdown.get() == "All":
            self.date_filter_mode_dropdown.set("Custom Date Range")
            self._toggle_date_filters()
        else:
            self.apply_filters()

    def _toggle_date_filters(self, event=None):
        mode = self.date_filter_mode_dropdown.get()
        if mode == "All":
            self.from_date_entry.config(state='disabled')
            self.to_date_entry.config(state='disabled')
            self.from_date_entry.delete(0, tk.END)
            self.from_date_entry.insert(0, "All")
            self.to_date_entry.delete(0, tk.END)
            self.to_date_entry.insert(0, "All")
            self.from_date_entry.set_date(None)
            self.to_date_entry.set_date(None)
        elif mode == "Custom Date Range":
            self.from_date_entry.config(state='readonly')
            self.to_date_entry.config(state='readonly')
            if self.from_date_entry.get() == "All":
                self.from_date_entry.delete(0, tk.END)
            if self.to_date_entry.get() == "All":
                self.to_date_entry.delete(0, tk.END)

        self.apply_filters()

    def _show_meter_group_selection_popup(self):
        popup = ctk.CTkToplevel(self.root)
        popup.title("Select Meter Groups")
        popup.transient(self.root)
        popup.grab_set()
        popup.focus_set()

        self.root.update_idletasks()
        main_x = self.root.winfo_x()
        main_y = self.root.winfo_y()
        main_width = self.root.winfo_width()
        main_height = self.root.winfo_height()

        popup_width = 300
        popup_height = min(400, (len(METER_GROUPS) + 2) * 30 + 70)

        popup_x = main_x + (main_width // 2) - (popup_width // 2)
        popup_y = main_y + (main_height // 2) - (popup_height // 2)
        popup.geometry(f"{popup_width}x{popup_height}+{popup_x}+{popup_y}")


        scrollable_checkbox_frame = ctk.CTkScrollableFrame(popup, fg_color=("gray90", "gray20"), corner_radius=8)
        scrollable_checkbox_frame.pack(side="top", fill="both", expand=True, padx=10, pady=10)

        popup_all_var = tk.BooleanVar(value=True)

        popup_vars = {}
        for group in METER_GROUPS:
            current_state = self.meter_group_filter_vars.get(group, tk.BooleanVar(value=True)).get()
            popup_vars[group] = tk.BooleanVar(value=current_state)

        def update_popup_all_var_state():
            current_states = [v.get() for v in popup_vars.values()]
            if not current_states:
                popup_all_var.set(False)
            elif all(current_states):
                popup_all_var.set(True)
            elif not any(current_states):
                popup_all_var.set(False)

        def popup_toggle_all_cmd():
            self._updating_meter_group_checkboxes_flag = True
            select_all = popup_all_var.get()
            for group_var in popup_vars.values():
                group_var.set(select_all)
            self._updating_meter_group_checkboxes_flag = False

        popup_all_checkbox = ctk.CTkCheckBox(
            scrollable_checkbox_frame,
            text="Select All / Clear All",
            variable=popup_all_var,
            command=popup_toggle_all_cmd,
            font=("Inter", 10, "bold")
        )
        popup_all_checkbox.pack(anchor='w', padx=5, pady=5)

        for group in METER_GROUPS:
            cb = ctk.CTkCheckBox(scrollable_checkbox_frame, text=group, variable=popup_vars[group], font=("Inter", 10))
            popup_vars[group].trace_add("write", lambda *args, pv=popup_vars, pa_var=popup_all_var:
                                                None if self._updating_meter_group_checkboxes_flag else update_popup_all_var_state())
            cb.pack(anchor='w', padx=5, pady=2)

        update_popup_all_var_state()

        button_frame = ctk.CTkFrame(popup, fg_color="transparent")
        button_frame.pack(pady=10)

        def on_apply():
            for group, var in popup_vars.items():
                self.meter_group_filter_vars[group].set(var.get())
            self._update_meter_group_display_text()
            popup.destroy()
            self.apply_filters()

        def on_cancel():
            popup.destroy()

        ctk.CTkButton(button_frame, text="Apply", command=on_apply, width=100, height=30, font=("Inter", 10, "bold"), corner_radius=8).pack(side=tk.LEFT, padx=5)
        ctk.CTkButton(button_frame, text="Cancel", command=on_cancel, width=100, height=30, font=("Inter", 10, "bold"), corner_radius=8).pack(side=tk.LEFT, padx=5)

        popup.protocol("WM_DELETE_WINDOW", on_cancel)
        popup.bind("<Return>", lambda e: on_apply())
        popup.bind("<Escape>", lambda e: on_cancel())


    def _update_meter_group_display_text(self):
        selected_count = sum(1 for var in self.meter_group_filter_vars.values() if var.get())
        if selected_count == len(METER_GROUPS):
            self.meter_group_display_var.set("All Selected")
        elif selected_count == 0:
            self.meter_group_display_var.set("None Selected")
        else:
            self.meter_group_display_var.set(f"{selected_count} Selected")

    def _clear_graph_filter(self):
        self.last_graph_clicked_sr_numbers = []
        self.graph_filter_active = False
        self.graph_filter_info_label.configure(text="")
        self.clear_graph_filter_button.grid_remove()
        self.apply_filters()


    def on_closing(self):
        if messagebox.askokcancel("Quit", "Do you want to quit the application? This will also close the dashboard."):
            self.stop_dash_server()
            self.root.destroy()
            global manager
            if manager is not None:
                manager.shutdown()


    def start_dash_server(self):
        global dash_thread, server_is_running
        if not server_is_running:
            try:
                dash_thread = multiprocessing.Process(
                    target=dash1.start_dash_server_threadsafe, 
                    args=(self.shared_data_proxy, self.click_data_queue,), 
                    daemon=True
                )
                dash_thread.start()
                server_is_running = True
                print("Dash server process started.")
                time.sleep(2)
            except Exception as e:
                messagebox.showerror("Dash Server Error", f"Failed to start Dash server:\n{e}")
                server_is_running = False

    def stop_dash_server(self):
        global server_is_running, dash_thread
        if server_is_running and dash_thread and dash_thread.is_alive():
            print("Attempting to terminate Dash server process.")
            dash_thread.terminate()
            dash_thread.join(timeout=5)
            if dash_thread.is_alive():
                print("Warning: Dash process did not terminate gracefully.")
            server_is_running = False
        else:
            print("Dash server not running or already terminated.")


    def clear_search_placeholder(self, event):
        if self.search_entry.get() == self.search_entry.cget("placeholder_text"):
            self.search_entry.delete(0, tk.END)

    def restore_search_placeholder(self, event):
        if not self.search_entry.get():
            self.search_entry.insert(0, self.search_entry.cget("placeholder_text"))

    def get_date_range_for_ageing_category(self, category_label):
        today = pd.Timestamp.now().normalize()
        if category_label == "0-21 days":
            return today - timedelta(days=21), today
        elif category_label == "22-30 days":
            return today - timedelta(days=30), today - timedelta(days=22)
        elif category_label == "31-45 days":
            return today - timedelta(days=45), today - timedelta(days=31)
        elif category_label == "46-90 days":
            return today - timedelta(days=90), today - timedelta(days=46)
        elif category_label == "90+ days":
            return pd.Timestamp('1900-01-01'), today - timedelta(days=91)
        return None, None


    def load_excel(self, file_path=None):
        if file_path is None:
            file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsb *.xlsx *.xls")])
            if not file_path:
                return

        try:
            if file_path.endswith('.xlsb'):
                import pyxlsb
                df = pd.read_excel(file_path, engine='pyxlsb')
            else:
                df = pd.read_excel(file_path)

            df.columns = [clean_column_name(col) for col in df.columns]

            for col in TKINTER_DATE_COLUMNS:
                if col in df.columns:
                    # Added a pre-check to replace empty-looking strings with NaN before conversion
                    df[col] = df[col].replace('', np.nan).replace(' ', np.nan)
                    if pd.api.types.is_numeric_dtype(df[col]):
                        df[col] = pd.to_datetime(df[col], unit='D', origin='1899-12-30', errors='coerce')
                    else:
                        df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)

                    if pd.api.types.is_datetime64_any_dtype(df[col]) and df[col].dt.tz is not None:
                        df[col] = df[col].dt.tz_localize(None)

            # --- DEBUG START in ex1.py: Check GRN Date after initial load and date conversion ---
            tracking_sr_no_ex1 = "546691"
            cleaned_sr_no_col = clean_column_name("SR no.")
            cleaned_grn_date_col = clean_column_name("GRN date")

            print(f"\n--- DEBUG in ex1.py: Checking SR {tracking_sr_no_ex1} immediately after data load and date conversion ---")
            if cleaned_sr_no_col in df.columns and tracking_sr_no_ex1 in df[cleaned_sr_no_col].astype(str).values:
                sr_data_at_load = df[df[cleaned_sr_no_col].astype(str) == tracking_sr_no_ex1]
                if not sr_data_at_load.empty:
                    grn_date_value_at_load = sr_data_at_load.iloc[0].get(cleaned_grn_date_col)
                    grn_date_is_na_at_load = pd.isna(grn_date_value_at_load)
                    print(f"  GRN Date value for {tracking_sr_no_ex1} at load: '{grn_date_value_at_load}'")
                    print(f"  Type of GRN Date value at load: {type(grn_date_value_at_load)}")
                    print(f"  pd.isna(GRN Date value at load): {grn_date_is_na_at_load}")
                    print(f"  Is GRN Date value at load a string? {isinstance(grn_date_value_at_load, str)}")
                else:
                    print(f"  SR {tracking_sr_no_ex1} found in column but row not retrieved (internal error or not unique).")
            else:
                print(f"  SR {tracking_sr_no_ex1} not found in '{cleaned_sr_no_col}' column at this stage in ex1.py.")
            print("-" * 50)
            # --- DEBUG END in ex1.py ---


            if 'SR no.' in df.columns:
                df['SR no.'] = df['SR no.'].astype(str)
            else:
                messagebox.showwarning("Missing Column", "The 'SR no.' column was not found in the Excel file after cleaning. Some functionalities may be impacted.")
            
            if 'SR status' in df.columns:
                df['Simplified SR Status'] = df['SR status'].astype(str).str.lower().apply(
                    lambda x: 'closed' if x == 'closed' else 'open'
                ).str.capitalize()
            else:
                df['Simplified SR Status'] = 'N/A'


            if 'Product family' in df.columns:
                df['Product Family Group'] = df['Product family'].apply(map_product_family)
            else:
                if 'Product Family Group' not in df.columns:
                    df['Product Family Group'] = "Other / Uncategorized"
                else:
                    df['Product Family Group'] = df['Product Family Group'].fillna("Other / Uncategorized")


            self.df_original = df.copy()
            messagebox.showinfo("Success", f"Successfully loaded {len(self.df_original)} rows from:\n{os.path.basename(file_path)}")

            self.populate_sr_type_dropdown()
            self._clear_graph_filter()
            self.apply_filters()

        except FileNotFoundError:
            messagebox.showerror("Error", "File not found.")
        except pd.errors.EmptyDataError:
            messagebox.showerror("Error", "The Excel file is empty.")
        except Exception as e:
            if "KeyError" in str(e) and any(req_col in str(e) for req_col in ["SR no.", "SR creation date", "GRN date"]):
                messagebox.showerror("Error", f"Failed to load Excel file: A critical column such as 'SR no.', 'SR creation date', or 'GRN date' was not found after cleaning column names. Please check your Excel file's headers.\nDetails: {e}")
            else:
                messagebox.showerror("Error", f"Failed to load Excel file:\n{e}")
            self.df_original = None
            self.df_filtered_for_table = get_empty_dataframe_structure_f1()
            self.df_filtered_for_charts = get_empty_dataframe_structure_f1()
            self.update_table_view(self.df_filtered_for_table)
            self.update_dash_data()

    def populate_sr_type_dropdown(self):
        options = ["All"]
        if self.df_original is not None and 'SR Problem type' in self.df_original.columns:
            unique_types = self.df_original['SR Problem type'].dropna().astype(str).unique()
            options.extend(sorted(unique_types))
        self.sr_type_dropdown.configure(values=options)


    def update_table_view(self, df):
        new_df_columns = list(df.columns)

        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = new_df_columns
        for col in new_df_columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=max(100, len(col) * 9), minwidth=40, anchor='w')
        
        self.tree.tag_configure('graph_highlight', background=self.graph_highlight_color, foreground="black")

        if df.empty:
            for col in new_df_columns:
                self.tree.column(col, width=100)
            return

        first_highlighted_item_id = None
        
        for i, (original_idx, row) in enumerate(df.iterrows()):
            display_values = []
            for val in row.values:
                if isinstance(val, pd.Timestamp):
                    display_values.append(val.strftime('%d-%b-%Y'))
                elif pd.isna(val):
                    display_values.append("")
                else:
                    display_values.append(str(val))

            tags_for_this_row = []
            tags_for_this_row.append('oddrow' if i % 2 == 0 else 'evenrow')
            
            if self.graph_filter_active:
                tags_for_this_row.append('graph_highlight')
                if first_highlighted_item_id is None:
                    first_highlighted_item_id = str(original_idx)

            self.tree.insert("", tk.END, iid=str(original_idx), text=str(i + 1), values=display_values, tags=tags_for_this_row)
        
        if self.graph_filter_active and first_highlighted_item_id:
            self.tree.see(first_highlighted_item_id)
            self.tree.focus(first_highlighted_item_id)
            self.tree.selection_set(first_highlighted_item_id)
        
        for i, col in enumerate(new_df_columns):
            header_width = len(col) * 9 + 30

            content_width = 0
            if not df.empty:
                sample_data = df.iloc[:min(200, len(df))][col].astype(str)
                if not sample_data.empty:
                    content_width = sample_data.apply(len).max() * 7 + 20

            min_col_width = 80
            final_width = max(min_col_width, header_width, content_width)
            self.tree.column(col, width=final_width, anchor='w', stretch=False)

    def apply_filters(self, event=None):
        if self.df_original is None:
            self.df_filtered_for_table = get_empty_dataframe_structure_f1()
            self.df_filtered_for_charts = get_empty_dataframe_structure_f1()
            self.update_table_view(self.df_filtered_for_table)
            self.update_dash_data()
            return

        df_base = self.df_original.copy()
        
        if 'SR status' in df_base.columns and 'Simplified SR Status' not in df_base.columns:
            df_base['Simplified SR Status'] = df_base['SR status'].astype(str).str.lower().apply(
                lambda x: 'closed' if x == 'closed' else 'open'
            ).str.capitalize()
        elif 'Simplified SR Status' not in df_base.columns:
            df_base['Simplified SR Status'] = 'N/A'

        df_temp_for_table = df_base.copy()
        
        # --- MODIFIED LOGIC ---
        # If a graph filter is active, it overrides all other filters.
        if self.graph_filter_active and self.last_graph_clicked_sr_numbers and 'SR no.' in df_temp_for_table.columns:
            df_temp_for_table['SR no.'] = df_temp_for_table['SR no.'].astype(str)
            df_temp_for_table = df_temp_for_table[df_temp_for_table['SR no.'].isin(self.last_graph_clicked_sr_numbers)].copy()
            self.graph_filter_info_label.configure(text=f"Graph Filter Active: Showing {df_temp_for_table.shape[0]} matching rows.")
            self.clear_graph_filter_button.grid()
        
        # Otherwise, if no graph filter is active, apply the GUI filters.
        else:
            self.graph_filter_info_label.configure(text="")
            self.clear_graph_filter_button.grid_remove()

            mode = self.date_filter_mode_dropdown.get()
            if mode == "Custom Date Range" and 'SR creation date' in df_temp_for_table.columns:
                from_date_obj = self.from_date_entry.get_date()
                to_date_obj = self.to_date_entry.get_date()
                if from_date_obj is not None:
                    from_date_ts = pd.Timestamp(from_date_obj).normalize()
                    df_temp_for_table = df_temp_for_table[df_temp_for_table['SR creation date'].dt.normalize() >= from_date_ts].copy()
                if to_date_obj is not None:
                    to_date_ts = pd.Timestamp(to_date_obj).normalize()
                    df_temp_for_table = df_temp_for_table[df_temp_for_table['SR creation date'].dt.normalize() <= to_date_ts + timedelta(days=1, microseconds=-1)].copy()

            if 'Product Family Group' in df_temp_for_table.columns and self.meter_group_filter_vars:
                selected_meter_groups = [group for group, var in self.meter_group_filter_vars.items() if var.get()]
                if len(selected_meter_groups) < len(METER_GROUPS):
                    if selected_meter_groups:
                        df_temp_for_table = df_temp_for_table[df_temp_for_table['Product Family Group'].isin(selected_meter_groups)].copy()
                    else:
                        df_temp_for_table = df_temp_for_table.iloc[0:0]

            sr_type = self.sr_type_dropdown.get()
            if sr_type != "All" and 'SR Problem type' in df_temp_for_table.columns:
                df_temp_for_table = df_temp_for_table[df_temp_for_table['SR Problem type'] == sr_type].copy()

            simplified_sr_status = self.sr_status_dropdown.get()
            if simplified_sr_status != "All" and 'Simplified SR Status' in df_temp_for_table.columns:
                df_temp_for_table = df_temp_for_table[df_temp_for_table['Simplified SR Status'] == simplified_sr_status].copy()

        # --- END OF MODIFIED LOGIC ---
        
        self.df_filtered_for_table = df_temp_for_table.copy()
        
        # THIS IS THE CORRECTED LOGIC: Send the full, filtered data to Dash.
        # The de-duplication will now happen inside Dash where needed.
        self.df_filtered_for_charts = self.df_filtered_for_table.copy()
        
        if not self.df_filtered_for_table.empty:
            self.df_filtered_for_table = self.df_filtered_for_table.sort_values(
                by=['SR creation date', 'SR no.'], ascending=[True, True]
            ).copy()

        self.filter_table_data() 

        if self.df_original is not None:
            self.update_dash_data()

    def filter_table_data(self, event=None):
        if self.df_filtered_for_table is None:
            self.update_table_view(get_empty_dataframe_structure_f1())
            return

        current_search_entry_text = self.search_entry.get()

        if not current_search_entry_text or current_search_entry_text == self.search_entry.cget("placeholder_text"):
            self.update_table_view(self.df_filtered_for_table)
            return
        
        search_text = current_search_entry_text.strip().lower()
        df_searchable = self.df_filtered_for_table.astype(str).apply(lambda col: col.str.lower())
        mask = df_searchable.apply(
            lambda row: row.str.contains(search_text, na=False).any(),
            axis=1
        )
        filtered_by_search_df = self.df_filtered_for_table[mask].copy()
        self.update_table_view(filtered_by_search_df)

    def start_editing_cell(self, event):
        if self.editing_entry is not None:
            self.editing_entry.destroy()
            self.editing_entry = None

        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return

        row_id = self.tree.identify_row(event.y)
        col = self.tree.identify_column(event.x)

        if col == '#0':
            return

        if not row_id or not col:
            return

        x, y, width, height = self.tree.bbox(row_id, col)
        col_index = int(col.replace('#', '')) - 1

        current_values_list = list(self.tree.item(row_id, 'values'))
        current_display_value = current_values_list[col_index] if col_index < len(current_values_list) else ""

        self.editing_entry = ttk.Entry(self.tree)
        self.editing_entry.place(x=x, y=y, width=width, height=height)
        self.editing_entry.insert(0, current_display_value)
        self.editing_entry.focus_set()

        def finish_edit(event=None):
            new_value_str = self.editing_entry.get()

            original_df_idx = int(row_id)
            col_name = self.tree["columns"][col_index]

            current_tree_values_on_entry_start = list(self.tree.item(row_id, 'values'))

            try:
                converted_value = new_value_str

                if self.df_original is None or col_name not in self.df_original.columns:
                    raise ValueError(f"Column '{col_name}' not found in original DataFrame.")

                original_dtype = self.df_original[col_name].dtype

                if pd.api.types.is_datetime64_any_dtype(original_dtype) or col_name in TKINTER_DATE_COLUMNS:
                    if new_value_str.strip() == "":
                        converted_value = pd.NaT
                    else:
                        converted_value = pd.to_datetime(new_value_str, dayfirst=True, errors='coerce')
                        if pd.isna(converted_value):
                            raise ValueError(f"'{new_value_str}' is not a valid date format. Please use DD-Mon-YYYY.")
                        if pd.api.types.is_datetime64_any_dtype(converted_value) and converted_value.tz is not None:
                            converted_value = converted_value.tz_localize(None)

                elif pd.api.types.is_numeric_dtype(original_dtype) or original_dtype == 'Int64':
                    if new_value_str.strip() == "":
                        converted_value = pd.NA
                    else:
                        try:
                            if original_dtype == 'Int64':
                                converted_value = pd.to_numeric(new_value_str, errors='raise').astype('Int64')
                            else:
                                converted_value = original_dtype.type(new_value_str)
                        except ValueError:
                            raise ValueError(f"'{new_value_str}' is not a valid number for this column.")
                else:
                    converted_value = new_value_str if new_value_str.strip() != "" else None


                self.df_original.loc[original_df_idx, col_name] = converted_value

                self.apply_filters(event=None)

            except ValueError as ve:
                messagebox.showerror("Invalid Input", str(ve))
                self.tree.item(row_id, values=current_tree_values_on_entry_start)
            except KeyError:
                messagebox.showerror("Error", f"Original DataFrame index {original_df_idx} not found. Data might have been reindexed unexpectedly.")
                self.tree.item(row_id, values=current_tree_values_on_entry_start)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to update data: {e}\nValue might not be valid for this column type or an unexpected error occurred.")
                self.tree.item(row_id, values=current_tree_values_on_entry_start)

            finally:
                if self.editing_entry:
                    self.editing_entry.destroy()
                    self.editing_entry = None

        self.editing_entry.bind("<Return>", finish_edit)
        self.editing_entry.bind("<FocusOut>", finish_edit)

    def export_filtered_data(self):
        if self.df_filtered_for_table is None or self.df_filtered_for_table.empty:
            messagebox.showwarning("No Data", "No data to export. Please load and filter data first.")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", "*.xlsx")]
        )
        if not file_path:
            return

        try:
            df_export = self.df_filtered_for_table.copy()

            for col in df_export.columns:
                if pd.api.types.is_datetime64_any_dtype(df_export[col]):
                    df_export[col] = df_export[col].dt.strftime('%d-%b-%Y')
                df_export[col] = df_export[col].fillna('')

            writer = pd.ExcelWriter(file_path, engine='xlsxwriter')
            df_export.to_excel(writer, sheet_name='Filtered_Data', index=False)

            workbook = writer.book
            worksheet = writer.sheets['Filtered_Data']

            for i, col in enumerate(df_export.columns):
                max_len = max(
                    len(str(col)),
                    df_export[col].astype(str).map(len).max() if not df_export[col].empty else 0
                ) + 2
                final_width = min(max_len * 1.2, 50)

                worksheet.set_column(i, i, final_width)

            writer.close()

            messagebox.showinfo("Success", "Filtered table data exported successfully with auto-adjusted column widths.")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to export Excel file:\n{e}")

    def export_unreceived_meters(self):
        if self.df_original is None or self.df_original.empty:
            messagebox.showwarning("No Data", "Please load an Excel file first to export unreceived meter data.")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", "*.xlsx")],
            initialfile="Unreceived_Meters_Categorized_Report.xlsx"
        )
        if not file_path:
            return

        try:
            success = export_unreceived_meters_report(self.df_original, file_path)
            if success:
                messagebox.showinfo("Export Complete", f"Successfully exported 'Unreceived Meter Data' report to:\n{file_path}")
            else:
                messagebox.showerror("Export Failed", "Failed to export 'Unreceived Meter Data' report. Check console for details.")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during 'Unreceived Meter Data' export:\n{e}\nCheck console for more details.")

    def export_unrepaired_meters_report_main(self):
        if self.df_original is None or self.df_original.empty:
            messagebox.showwarning("No Data", "Please load an Excel file first to export unrepaired meters report.")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", "*.xlsx")],
            initialfile="Unrepaired_Meters_Summary_Report.xlsx"
        )
        if not file_path:
            return

        try:
            success = export_unrepaired_meters_report(self.df_original, file_path)
            if success:
                messagebox.showinfo("Export Complete", f"Successfully exported 'Unrepaired Meters' report to:\n{file_path}")
            else:
                messagebox.showerror("Export Failed", "Failed to export 'Unrepaired Meters' report. Check console for details.")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during 'Unrepaired Meters' report export:\n{e}\nCheck console for more details.")

    def export_unclosed_repair_lines_main(self):
        if self.df_original is None or self.df_original.empty:
            messagebox.showwarning("No Data", "Please load an Excel file first to export unclosed repair lines report.")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", "*.xlsx")],
            initialfile="Unclosed_Repair_Lines_Report.xlsx"
        )
        if not file_path:
            return

        try:
            success = export_unclosed_repairs_report(self.df_original, file_path)
            if success:
                messagebox.showinfo("Export Complete", f"Successfully exported 'Unclosed Repair Lines' report to:\n{file_path}")
            else:
                messagebox.showerror("Export Failed", "Failed to export 'Unclosed Repair Lines' report. Check console for details.")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during 'Unclosed Repair Lines' report export:\n{e}\nCheck console for more details.")

    def export_sr_category_main(self):
        if self.df_original is None or self.df_original.empty:
            messagebox.showwarning("No Data", "Please load an Excel file first to export the SR Category report.")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", "*.xlsx")],
            initialfile="SR_Meter_Grouping_Report.xlsx"
        )
        if not file_path:
            return

        try:
            success = export_received_not_repaired_report(self.df_original, file_path)
            if success:
                messagebox.showinfo("Export Complete", f"Successfully exported 'SR Meter Grouping' report to:\n{file_path}")
            else:
                messagebox.showerror("Export Failed", "Failed to export 'SR Meter Grouping' report. Check console for details.")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred during 'SR Meter Grouping' report export:\n{e}\nCheck console for more details.")


    def show_plotly_dashboard(self):
        """Launches the web browser to the Dash dashboard."""
        global server_is_running
        if not server_is_running:
            messagebox.showwarning("Dash Server", "Dash server is not running. Attempting to start it.")
            self.start_dash_server()
            time.sleep(2)
            if not server_is_running:
                messagebox.showerror("Dash Server", "Could not start Dash server. Please check the console for errors.")
                return

        self.update_dash_data()

        dashboard_url = "http://127.0.0.1:8050/"
        try:
            webbrowser.open(dashboard_url)
        except Exception as e:
            messagebox.showerror("Web Browser Error", f"Could not open web browser:\n{e}")

    def update_dash_data(self):
        """Sends the currently filtered data to the Dash server."""
        global server_is_running
        if not server_is_running:
            print("Dash server not running, cannot update data.")
            return

        if self.df_filtered_for_charts is None:
            df_to_send = get_empty_dataframe_structure_f1()
        else:
            df_to_send = self.df_filtered_for_charts.copy()

        try:
            self.shared_data_proxy['live_data'] = process_df_to_json_f1(df_to_send)
            print(f"Data assigned to shared_data_proxy['live_data']: {len(df_to_send)} rows.")
        except Exception as e:
            print(f"ERROR: Failed to send data to Dash server via proxy:\n{e}")

    def _show_filter_confirmation_dialog(self, sr_count):
        """Displays a confirmation dialog and brings Tkinter to front on OK."""
        if sr_count > 0:
            title = "Graph Filter Applied"
            message = (f"{sr_count} unique SRs have been clicked on the graph. \n\n"
                       "The table will now be filtered to show all associated rows for these SRs. "
                       "Other filters (date, status, etc.) will also be applied.")
            self.root.after_idle(lambda: self.show_dialog_and_lift(title, message))
        else:
            title = "Graph Filter Information"
            message = "No SRs found for the selected graph segment after applying filters."
            self.root.after_idle(lambda: messagebox.showinfo(title, message))


    def show_dialog_and_lift(self, title, message):
        """Helper to show message box and then lift window."""
        result = messagebox.showinfo(title, message)
        if result == "ok":
            self.root.lift()
            self.root.attributes('-topmost', True)
            self.root.attributes('-topmost', False)

    def check_for_dash_clicks(self):
        """Periodically checks the shared queue for click data from Dash."""
        try:
            all_clicked_data = []
            while not self.click_data_queue.empty():
                all_clicked_data.append(self.click_data_queue.get_nowait())

            if all_clicked_data:
                latest_clicked_data = all_clicked_data[-1]
                sr_numbers = latest_clicked_data.get('sr_numbers', [])
                
                if isinstance(sr_numbers, list):
                    sr_numbers_processed = [str(s) for s in sr_numbers if s is not None]
                else:

                    sr_numbers_processed = [str(sr_numbers)] if sr_numbers is not None else []
                
                if sr_numbers_processed:
                    self.last_graph_clicked_sr_numbers = sr_numbers_processed
                    self.graph_filter_active = True
                    self.apply_filters()
                    self._show_filter_confirmation_dialog(len(sr_numbers_processed))
                else: 
                    self.graph_filter_active = False
                    self.apply_filters()

        except Empty:
            pass
        except Exception as e:
            print(f"Error checking for Dash clicks: {e}")

        self.root.after(1000, self.check_for_dash_clicks)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    manager = multiprocessing.Manager()
    shared_data_proxy = manager.dict()
    click_data_queue = manager.Queue()

    initial_empty_df = get_empty_dataframe_structure_f1().copy()

    shared_data_proxy['clicked_sr_numbers_from_dash'] = {} 

    initial_empty_df_json = process_df_to_json_f1(initial_empty_df)
    shared_data_proxy['live_data'] = initial_empty_df_json

    root = ctk.CTk()
    
    # This is a more reliable way to maximize the window on startup
    root.after(100, lambda: root.state('zoomed'))

    if getattr(sys, 'frozen', False):
        base_path = sys._MEIPASS
    else:
        base_path = os.path.abspath(".")

    app = ExcelApp(root, shared_data_proxy, click_data_queue)
    root.mainloop()