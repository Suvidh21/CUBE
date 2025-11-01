# dash1.py

# --- Standard Python Library Imports ---
import threading
import time
from datetime import datetime, timedelta, date # Added 'date'
import io
import warnings
import multiprocessing
import base64 # Import for Base64 encoding/decoding
import os
from io import BytesIO

# --- Third-Party Library Imports ---
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# --- Dash Specific Imports ---
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL

# --- Dash Bootstrap Components (dbc) Import ---
import dash_bootstrap_components as dbc

# --- Flask Caching Import ---
from flask_caching import Cache


# --- ReportLab Imports for PDF Generation ---
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.platypus import Image
from reportlab.lib.utils import ImageReader
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph, Spacer


# --- Global Configurations and Settings ---

# Suppress all FutureWarnings from Pandas to clean up console
warnings.simplefilter(action='ignore', category=FutureWarning)
# Suppress the specific UserWarning about infer_datetime_format' is deprecated
warnings.filterwarnings("ignore", message="^The argument 'infer_datetime_format' is deprecated")


# Adopt future Pandas behavior for silent downcasting
pd.set_option('future.no_silent_downcasting', True)


# Define common date columns that, need to be parsed
DATE_COLUMNS = [
    "SR creation date",
    "Incident Date",
    "GRN date",
    "Repair complete date",
    "SR closure date",
    "Ageing as on today from sales shipment",
    "Sales Shipment Date",
    "Inter-org challan date from Branch to Repair center",
    "Inter-org challan date from Repair center to Branch"
]

# --- Helper Functions (Independent of Dash app instance) ---

def get_empty_dataframe_structure():
    df = pd.DataFrame(columns=[
        'Branch name', 'SR no.', 'SR creation date', 'SR status', 'SR Problem type',
        'Product family', 'Product Family Group',
        'Diagnose code 1-Category', 'Diagnose code 2-Category',
        'Diagnose code 3-Category', 'GRN date', 'Inter-org challan date from Repair center to Branch',
        'SR closure date',
        'Ageing', 'Ageing Category',
        'M1_TAT', 'M2_TAT', 'M3_TAT',
        'M1_TAT_Category', 'M2_TAT_Category', 'M3_TAT_Category',
        'Overall_TAT', 'Overall_TAT_Category', 'MonthYear',
        'Simplified SR Status' # New column for simplified status
    ])
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.Series(dtype='datetime64[ns]')
    if 'SR no.' in df.columns:
        df['SR no.'] = pd.Series(dtype='object')

    for col in ['Ageing', 'M1_TAT', 'M2_TAT', 'M3_TAT', 'Overall_TAT']:
        if col in df.columns:
            df[col] = pd.Series(dtype='Int64')

    return df

app = dash.Dash(__name__, suppress_callback_exceptions=True,
                 external_stylesheets=[
                     dbc.themes.BOOTSTRAP,
                     'https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap',
                     'https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css'
                 ])

CACHE_CONFIG = {
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory',
    'TIMEOUT': 60
}
cache = Cache()
cache.init_app(app.server, config=CACHE_CONFIG)


bg_color = "#1A202C"
text_color = "#E2E8F0"
body_text_color = "#A0AEC0"
card_bg_color = "#FFFFFF"
card_text_color = "#2D3748"
card_body_text_color = "#4A5568"
card_border_radius = "12px"
box_shadow_css = "0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)"
max_container_width = "1600px"
headline_font_family = "'Inter', sans-serif"
body_font_family = "'Inter', sans-serif"

SINGLE_COLOR_SEQUENCE = ['#00BFFF']
QUALITATIVE_COLOR_SEQUENCE = px.colors.qualitative.Vivid

OVERALL_TAT_COLORS = {
    '<=21D': '#28a745',
    '21-30 D':  '#FFFF00',
    '>30D': '#dc3545',
    'OPEN': '#17a2b8',
    'Other Status': '#888888',
    'N/A Status/Date': '#cccccc'
}

# dash1.py

STAGE_TAT_COLORS = {
    '<7D': '#28a745',      # Green
    '7-10D': '#FFD700',    # Gold (Yellow)
    '11-15D': '#FF4500',   # OrangeRed
    '>15D': '#DC143C',     # Crimson
    'N/A': '#cccccc'
}

AGEING_COLORS_MAP = {
    '0-21 days': OVERALL_TAT_COLORS['<=21D'],
    '22-30 days': '#F08080',  # Light Coral (as previously set)
    '31-45 days': OVERALL_TAT_COLORS['>30D'],
    '46-90 days': '#8A2BE2',
    '90+ days': '#808080'   # Changed from '#6A5ACD' to '#808080' for grey
}

# --- NEW: Calculate default week value ---
today = date.today()
# This creates a string like '2025-W30' which matches the format in the dropdown options
current_week_value = today.strftime('%Y-W%U')


def ensure_datetime_columns(df, columns_to_check=DATE_COLUMNS):
    if df is None or not isinstance(df, pd.DataFrame):
        return get_empty_dataframe_structure()
    df_copy = df.copy()
    for col in columns_to_check:
        if col in df_copy.columns:
            if pd.api.types.is_object_dtype(df_copy[col]):
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce', infer_datetime_format=True)
            if pd.api.types.is_datetime64_any_dtype(df_copy[col]) and df_copy[col].dt.tz is not None:
                df_copy[col] = df_copy[col].dt.tz_localize(None)
    if 'SR no.' in df_copy.columns:
        df_copy['SR no.'] = df_copy['SR no.'].astype(str)
    return df_copy

def get_filter_value(value):
    if value is None or (isinstance(value, list) and not value) or \
       (isinstance(value, str) and (value.strip() == '' or value.strip().lower() == 'select...')):
        return 'All'
    return value

def process_df_to_json(df):
    return df.to_json(date_format='iso', orient='split')

def read_df_from_json(data_json):
    if data_json is None or data_json == "":
        return get_empty_dataframe_structure()
    try:
        dtype_mapping = {
            'Branch name': str, 'SR no.': str, 'SR status': str, 'SR Problem type': str,
            'Product family': str, 'Product Family Group': str,
            'Diagnose code 1-Category': str, 'Diagnose code 2-Category': str, 'Diagnose code 3-Category': str,
            'Ageing Category': str, 'M1_TAT_Category': str, 'M2_TAT_Category': str,
            'M3_TAT_Category': str, 'Overall_TAT_Category': str, 'MonthYear': str,
            'Simplified SR Status': str
        }
        for col in DATE_COLUMNS:
            dtype_mapping[col] = 'datetime64[ns]'
        df = pd.read_json(io.StringIO(data_json), orient='split', dtype=dtype_mapping)
        return ensure_datetime_columns(df, DATE_COLUMNS)
    except Exception as e:
        print(f"Error reading JSON to DataFrame in dash1.py: {e}")
        return get_empty_dataframe_structure()

def get_empty_figure_with_message(message="No data to display for this chart."):
    fig = go.Figure()
    fig.update_layout(
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        annotations=[
            dict(
                text=message,
                xref="paper", yref="paper",
                showarrow=False,
                font=dict(color=card_body_text_color, size=16),
                x=0.5, y=0.5
            )
        ],
        plot_bgcolor=card_bg_color,
        paper_bgcolor=card_bg_color,
        margin=dict(t=50, b=50, l=40, r=40)
    )
    return fig

# --- Chart Generation Functions ---

# THIS IS THE NEW DAILY PERFORMANCE CHART FUNCTION
def create_daily_performance_chart(df_input, selected_week):
    """
    Generates a daily performance chart for a selected week.
    """
    if selected_week is None or selected_week == 'All':
        return get_empty_figure_with_message("Please select a week to view its daily performance.")

    required_cols = ['GRN date', 'Inter-org challan date from Repair center to Branch']
    if df_input.empty or not all(col in df_input.columns for col in required_cols):
        return get_empty_figure_with_message("Required data is missing for daily performance analysis.")

    df = df_input.copy()
    for col in required_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    df.dropna(subset=required_cols, how='any', inplace=True)
    if df.empty:
        return get_empty_figure_with_message("No valid data for the selected filters.")

    # --- 2. Setup Date Range for the selected week ---
    try:
        start_of_week = pd.to_datetime(selected_week + '-0', format='%Y-W%U-%w').normalize()
        end_of_week = start_of_week + timedelta(days=6)
        days_of_week_range = pd.date_range(start=start_of_week, end=end_of_week, freq='D')
        days_of_week_labels = days_of_week_range.strftime('%A')
    except ValueError:
        return get_empty_figure_with_message("Invalid week format selected.")


    # --- 3. Data Preparation ---
    # Active Backlog calculation
    received_before_week = df[df['GRN date'] < start_of_week].shape[0]
    repaired_before_week = df[df['Inter-org challan date from Repair center to Branch'] < start_of_week].shape[0]
    initial_backlog = received_before_week - repaired_before_week

    # Filter data for daily calculations within the week
    df_week_received = df[df['GRN date'].dt.normalize().between(start_of_week, end_of_week, inclusive='both')]
    df_week_repaired = df[df['Inter-org challan date from Repair center to Branch'].dt.normalize().between(start_of_week, end_of_week, inclusive='both')]

    # Daily counts for the selected week
    daily_received = df_week_received.groupby(df_week_received['GRN date'].dt.normalize()).size().reindex(days_of_week_range, fill_value=0)
    daily_repaired = df_week_repaired.groupby(df_week_repaired['Inter-org challan date from Repair center to Branch'].dt.normalize()).size().reindex(days_of_week_range, fill_value=0)

    # Cumulative repaired for the selected week
    cumulative_repaired_week = daily_repaired.cumsum()

    # Average repair time for meters completed on a specific day
    df_week_repaired['Repair Time'] = (df_week_repaired['Inter-org challan date from Repair center to Branch'] - df_week_repaired['GRN date']).dt.days
    avg_repair_time_daily = df_week_repaired.groupby(df_week_repaired['Inter-org challan date from Repair center to Branch'].dt.normalize())['Repair Time'].mean().reindex(days_of_week_range)

    # Daily backlog for the selected week
    cumulative_received_week = daily_received.cumsum()
    daily_backlog = initial_backlog + cumulative_received_week - cumulative_repaired_week

    # --- 4. Create the Combination Chart ---
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add Red Area Chart for Active Backlog (Left Y-Axis)
    fig.add_trace(go.Scatter(
        x=days_of_week_labels, y=daily_backlog, name='Active Backlog',
        mode='lines', line=dict(color='red', width=0),
        fill='tozeroy', fillcolor='rgba(220, 53, 69, 0.4)',
        hovertemplate='<b>%{x}</b><br>Active Backlog: %{y}<extra></extra>'
    ), secondary_y=False)

    # Add Green Area Chart for Cumulative Meters Repaired (Left Y-Axis)
    fig.add_trace(go.Scatter(
        x=days_of_week_labels, y=cumulative_repaired_week, name='Cumulative Meters Repaired',
        mode='lines', line=dict(color='green', width=0),
        fill='tozeroy', fillcolor='rgba(40, 167, 69, 0.4)',
        hovertemplate='<b>%{x}</b><br>Cumulative Repaired: %{y}<extra></extra>'
    ), secondary_y=False)

    # Add Blue Line Chart for Meters Received (Left Y-Axis)
    fig.add_trace(go.Scatter(
        x=days_of_week_labels, y=daily_received, name='Meters Received (Daily)',
        mode='lines+markers', line=dict(color='blue'),
        hovertemplate='<b>%{x}</b><br>Received: %{y}<extra></extra>'
    ), secondary_y=False)

    # Add Black Line Chart for Meters Repaired (Left Y-Axis)
    fig.add_trace(go.Scatter(
        x=days_of_week_labels, y=daily_repaired, name='Meters Repaired (Daily)',
        mode='lines+markers', line=dict(color='black'),
        hovertemplate='<b>%{x}</b><br>Repaired: %{y}<extra></extra>'
    ), secondary_y=False)

    # Add Orange Line Chart for Average Repair Time (Right Y-Axis)
    fig.add_trace(go.Scatter(
        x=days_of_week_labels, y=avg_repair_time_daily, name='Average Repair Time',
        mode='lines+markers', line=dict(color='orange', dash='dash'),
        hovertemplate='<b>%{x}</b><br>Avg Repair Time: %{y:.1f} days<extra></extra>'
    ), secondary_y=True)

    # --- 5. Style the Chart ---
    fig.update_layout(
        title_text=f"Daily Performance for Week of {start_of_week.strftime('%B %d, %Y')}",
        plot_bgcolor=card_bg_color, paper_bgcolor=card_bg_color,
        font=dict(family=body_font_family, color=card_body_text_color, size=12),
        margin=dict(t=80, b=100, l=60, r=60),
        title_font=dict(size=20, color=card_text_color, family=headline_font_family), title_x=0.5,
        legend=dict(orientation="h", yanchor="bottom", y=-0.4, xanchor="center", x=0.5),
        xaxis_title="Day of the Week",
        yaxis=dict(title="Count of Meters", color=card_text_color),
        yaxis2=dict(title="Average Days", color=card_text_color, overlaying='y', side='right', showgrid=False),
        hovermode='x unified'
    )

    return fig


# THIS IS THE FINAL SIMPLIFIED WEEKLY CHART FUNCTION
def create_simplified_weekly_chart(df_input):
    if df_input.empty:
        return get_empty_figure_with_message("No data available for weekly performance analysis.")

    df = df_input.copy()

    date_cols_to_check = ['SR creation date', 'GRN date', 'Inter-org challan date from Repair center to Branch']
    for col in date_cols_to_check:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        else:
            return get_empty_figure_with_message(f"Missing required date column: {col}")

    df.dropna(subset=date_cols_to_check, how='any', inplace=True)
    if df.empty:
        return get_empty_figure_with_message("No data with complete date information for this chart.")

    # --- 1. Prepare Weekly Data ---
    df['ReceivedWeek'] = df['GRN date'].dt.strftime('%Y-W%U')
    df['DispatchedWeek'] = df['Inter-org challan date from Repair center to Branch'].dt.strftime('%Y-W%U')

    received_per_week = df.groupby('ReceivedWeek').size()
    dispatched_per_week = df.groupby('DispatchedWeek').size()

    # --- 2. Prepare Efficiency and Backlog Data ---
    df['Time_to_Receipt'] = (df['GRN date'] - df['SR creation date']).dt.days
    avg_receipt_time = df.groupby('ReceivedWeek')['Time_to_Receipt'].mean()

    # Combine weekly data into a single DataFrame
    weekly_df = pd.DataFrame({
        'Meters Received': received_per_week,
        'Meters Repaired': dispatched_per_week
    }).sort_index().fillna(0)

    # Calculate active backlog trend
    weekly_df['Active Backlog'] = (weekly_df['Meters Received'].cumsum() - weekly_df['Meters Repaired'].cumsum())

    # Join efficiency data
    weekly_df = weekly_df.join(avg_receipt_time.rename('Avg Time to Receive'))

    # --- Filter to show only the last 4 weeks of data ---
    if not weekly_df.empty:
        all_weeks = weekly_df.index.tolist()
        weeks_to_display = all_weeks[-4:]
        weekly_df = weekly_df.loc[weeks_to_display]

    if weekly_df.empty:
        return get_empty_figure_with_message("No weekly data available for the most recent 4 weeks.")

    # --- NEW: Format the X-axis labels to show dates ---
    # This converts strings like '2025-W29' to 'Week of Jul 20, 2025'
    x_axis_labels = [f'Week of {pd.to_datetime(w + "-0", format="%Y-W%U-%w").strftime("%b %d, %Y")}' for w in weekly_df.index]


    # --- 3. Create the Combination Chart ---
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add Bar Traces for weekly volume
    fig.add_trace(go.Bar(
        x=x_axis_labels, y=weekly_df['Meters Received'], name='Meters Received', # Use new labels
        marker_color='green'
    ), secondary_y=False)

    fig.add_trace(go.Bar(
        x=x_axis_labels, y=weekly_df['Meters Repaired'], name='Meters Repaired', # Use new labels
        marker_color='royalblue'
    ), secondary_y=False)

    # Add Line Trace for backlog on the primary axis
    fig.add_trace(go.Scatter(
        x=x_axis_labels, y=weekly_df['Active Backlog'], name='Active Backlog', # Use new labels
        mode='lines+markers', line=dict(color='red', width=3)
    ), secondary_y=False)

    # Add Line Trace for efficiency on the secondary axis
    fig.add_trace(go.Scatter(
        x=x_axis_labels, y=weekly_df['Avg Time to Receive'], name='Avg Time to Receive', # Use new labels
        mode='lines+markers', line=dict(color='orange', dash='dash')
    ), secondary_y=True)

    # --- 4. Style the Chart ---
    fig.update_layout(
        title_text="Weekly Repair Center Dashboard (Last 4 Weeks)",
        plot_bgcolor=card_bg_color, paper_bgcolor=card_bg_color,
        font=dict(family=body_font_family, color=card_body_text_color, size=12),
        margin=dict(t=80, b=100, l=60, r=60),
        title_font=dict(size=20, color=card_text_color, family=headline_font_family), title_x=0.5,
        legend=dict(orientation="h", yanchor="bottom", y=-0.4, xanchor="center", x=0.5),
        xaxis_title="Week",
        yaxis=dict(title="Count of Meters"),
        yaxis2=dict(title="Average Days", overlaying='y', side='right', showgrid=False),
        barmode='group',
        hovermode='x unified'
    )
    return fig

def create_srs_per_branch_chart(df_unique_srs_input):
    if 'Branch name' not in df_unique_srs_input.columns or df_unique_srs_input.empty or df_unique_srs_input['Branch name'].dropna().empty:
        return get_empty_figure_with_message("No 'Branch name' data to display.")

    branch_counts = df_unique_srs_input['Branch name'].value_counts().reset_index()
    branch_counts.columns = ['Branch', 'Count']

    # Using plotly.graph_objects for more stability
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=branch_counts['Branch'],
        y=branch_counts['Count'],
        marker_color='#00BFFF'
    ))

    fig.update_layout(
        title='SRs per Branch',
        plot_bgcolor=card_bg_color,
        paper_bgcolor=card_bg_color,
        font=dict(family=body_font_family, color=card_body_text_color, size=12),
        margin=dict(t=50, b=50, l=40, r=40),
        title_font=dict(size=20, color=card_text_color, family=headline_font_family),
        title_x=0.5,
        xaxis=dict(title="Branch", title_font=dict(color=card_text_color)),
        yaxis=dict(title="Count", tickformat=".2s", title_font=dict(color=card_text_color)),
        transition_duration=1000
    )
    return fig

def create_sr_status_distribution_chart(df_unique_srs_input):
    if 'Simplified SR Status' not in df_unique_srs_input.columns or df_unique_srs_input.empty or df_unique_srs_input['Simplified SR Status'].dropna().empty:
        return get_empty_figure_with_message("No 'Simplified SR Status' data to display.")

    all_simplified_statuses = ['Open', 'Closed', 'N/A']
    status_counts = df_unique_srs_input['Simplified SR Status'].value_counts().reindex(all_simplified_statuses, fill_value=0).reset_index()
    status_counts.columns = ['Status', 'Count']

    status_sort_order = ['Open', 'Closed', 'N/A']
    status_counts['Status'] = pd.Categorical(status_counts['Status'], categories=status_sort_order, ordered=True)
    status_counts = status_counts.sort_values('Status')

    sr_status_colors = { 'Open': '#dc3545', 'Closed': '#28a745', 'N/A': '#888888' }
    fig = px.pie(status_counts, values='Count', names='Status', hole=0.45, title='Simplified SR Status Distribution',
                             color_discrete_map=sr_status_colors,
                             color='Status')
    fig.update_layout(
        plot_bgcolor=card_bg_color, paper_bgcolor=card_bg_color,
        font=dict(family=body_font_family, color=card_body_text_color, size=12),
        margin=dict(t=50, b=50, l=40, r=40),
        title_font=dict(size=20, color=card_text_color, family=headline_font_family),
        title_x=0.5,
        legend_title_text='Status',
        legend=dict(font=dict(color=card_body_text_color)),
        transition_duration=1000
    )
    return fig

def create_sr_status_by_ageing_category_chart(df_input, selected_ageing_category='All'):
    if 'Ageing Category' not in df_input.columns or 'Simplified SR Status' not in df_input.columns or df_input.empty:
        return get_empty_figure_with_message("No 'Ageing Category' or 'Simplified SR Status' data for Ageing analysis.")

    ageing_df_plot = df_input.copy()

    # THIS FUNCTION NOW EXPECTS THE FULL DATA, SO WE DE-DUPLICATE INTERNALLY
    if 'SR no.' in ageing_df_plot.columns:
        # Keep the last entry for each SR to get its most recent status
        ageing_df_plot = ageing_df_plot.sort_values('SR creation date', ascending=True).drop_duplicates(subset=['SR no.'], keep='last')

    if isinstance(selected_ageing_category, list) and selected_ageing_category and 'All' not in selected_ageing_category:
        ageing_df_plot = ageing_df_plot[ageing_df_plot['Ageing Category'].isin(selected_ageing_category)]
    elif isinstance(selected_ageing_category, str) and selected_ageing_category != 'All':
        ageing_df_plot = ageing_df_plot[ageing_df_plot['Ageing Category'] == selected_ageing_category]

    if ageing_df_plot.empty:
        return get_empty_figure_with_message("No valid data for Ageing analysis after filtering and dropping NaNs.")

    ageing_labels = ['0-21 days', '22-30 days', '31-45 days', '46-90 days', '90+ days', 'N/A']
    ageing_color_map = AGEING_COLORS_MAP
    ageing_color_map['N/A'] = ageing_color_map.get('N/A', '#cccccc')

    sr_status_order = ['Open', 'Closed', 'N/A']

    grouped_data = ageing_df_plot.groupby(['Simplified SR Status', 'Ageing Category']).size().reset_index(name='Count')
    grouped_data['Ageing Category'] = pd.Categorical(grouped_data['Ageing Category'], categories=ageing_labels, ordered=True)
    grouped_data['Simplified SR Status'] = pd.Categorical(grouped_data['Simplified SR Status'], categories=sr_status_order, ordered=True)
    grouped_data = grouped_data.sort_values(by=['Simplified SR Status', 'Ageing Category'])

    fig = go.Figure()

    title_str = "SR Status by Ageing Category"

    if (isinstance(selected_ageing_category, list) and selected_ageing_category and 'All' not in selected_ageing_category) or \
       (isinstance(selected_ageing_category, str) and selected_ageing_category != 'All'):
        filtered_for_display_data = grouped_data.copy()
        if filtered_for_display_data.empty:
            return get_empty_figure_with_message(f"No data for SR Status for selected Ageing Categories.")

        for status in sr_status_order:
            status_data = filtered_for_display_data[filtered_for_display_data['Simplified SR Status'] == status]
            if not status_data.empty:
                for index, row in status_data.iterrows():
                    ageing_cat = row['Ageing Category']
                    count_val = row['Count']
                    sr_numbers_for_this_segment = ageing_df_plot[
                        (ageing_df_plot['Simplified SR Status'] == status) &
                        (ageing_df_plot['Ageing Category'] == ageing_cat)
                    ]['SR no.'].dropna().unique().tolist()
                    fig.add_trace(go.Bar(
                        x=[status], y=[count_val], name=ageing_cat,
                        marker_color=ageing_color_map.get(ageing_cat, '#888888'),
                        showlegend=True, opacity=0.95,
                        hovertemplate='<b>Status:</b> %{x}<br><b>Ageing:</b> %{fullData.name}<br><b>Count:</b> %{y}<extra></extra>',
                        customdata=[sr_numbers_for_this_segment]
                    ))
        fig.update_layout(barmode='stack')
    else:
        if grouped_data.empty:
            return get_empty_figure_with_message("No data for SR Status by Ageing Category: All.")

        for category in ageing_labels:
            current_category_data = grouped_data[grouped_data['Ageing Category'] == category]
            full_x_axis_df = pd.DataFrame({'Simplified SR Status': sr_status_order})
            category_data_for_plot = pd.merge(full_x_axis_df, current_category_data, on='Simplified SR Status', how='left').fillna({'Count': 0})
            category_data_for_plot['Simplified SR Status'] = pd.Categorical(category_data_for_plot['Simplified SR Status'], categories=sr_status_order, ordered=True)
            category_data_for_plot = category_data_for_plot.sort_values(by='Simplified SR Status')

            y_counts_for_category = category_data_for_plot['Count'].tolist()

            sr_numbers_for_this_trace = []
            for status in sr_status_order:
                sr_numbers_for_segment = ageing_df_plot[
                    (ageing_df_plot['Simplified SR Status'] == status) &
                    (ageing_df_plot['Ageing Category'] == category)
                ]['SR no.'].dropna().unique().tolist()
                sr_numbers_for_this_trace.append(sr_numbers_for_segment)
            fig.add_trace(go.Bar(
                x=category_data_for_plot['Simplified SR Status'],
                y=y_counts_for_category, name=category,
                marker_color=ageing_color_map.get(category, '#888888'),
                legendgroup=category, showlegend=True, opacity=0.95,
                hovertemplate='<b>Status:</b> %{x}<br><b>Ageing:</b> %{fullData.name}<br><b>Count:</b> %{y}<extra></extra>',
                customdata=sr_numbers_for_this_trace
            ))
        fig.update_layout(barmode='stack')
    fig.update_layout(
        title=title_str,
        xaxis_title="SR Status", yaxis_title="Count of Service Requests",
        plot_bgcolor=card_bg_color, paper_bgcolor=card_bg_color,
        font=dict(family=body_font_family, color=card_body_text_color, size=12),
        legend_title_text='Ageing Category',
        legend=dict(
            bordercolor="#e5e7eb", borderwidth=1, bgcolor=card_bg_color,
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
            font=dict(color=card_body_text_color)
        ),
        margin=dict(t=100, b=50, l=60, r=40), bargap=0.15, bargroupgap=0.05, hovermode='closest',
        title_font=dict(size=20, color=card_text_color, family=headline_font_family), title_x=0.5,
        xaxis=dict(categoryorder='array', categoryarray=sr_status_order, title_font=dict(color=card_text_color)),
        yaxis=dict(tickformat=".2s", showgrid=True, gridwidth=1, gridcolor='#e0e0e0', title_font=dict(color=card_text_color)),
        transition_duration=1000
    )
    fig.update_traces(marker_line_width=0.5, marker_line_color='rgba(0,0,0,0.15)')
    return fig

def create_srs_over_time_chart(df_unique_srs_input):
    if 'SR creation date' not in df_unique_srs_input.columns or df_unique_srs_input.empty or df_unique_srs_input['SR creation date'].dropna().empty:
        return get_empty_figure_with_message("No 'SR creation date' data to display.")
    df_time = df_unique_srs_input
    if not df_time.empty:
        df_time['Month'] = df_time['SR creation date'].dt.to_period('M').astype(str)
        time_series = df_time.groupby('Month').size().reset_index(name='SR Count')
        fig = px.line(time_series, x='Month', y='SR Count', markers=True, title='Monthly SR Volume',
                                   line_shape='spline', color_discrete_sequence=SINGLE_COLOR_SEQUENCE)
        fig.update_layout(
            plot_bgcolor=card_bg_color, paper_bgcolor=card_bg_color,
            font=dict(family=body_font_family, color=card_body_text_color, size=12),
            margin=dict(t=50, b=50, l=60, r=40),
            title_font=dict(size=20, color=card_text_color, family=headline_font_family), title_x=0.5,
            xaxis_title="Month", yaxis_title="SR Count",
            yaxis=dict(title="Count", tickformat=".2s", title_font=dict(color=card_text_color)),
            xaxis=dict(title_font=dict(color=card_text_color)),
            transition_duration=1000
        )
        fig.update_xaxes(tickangle=45)
        return fig
    return get_empty_figure_with_message("No 'SR creation date' data after filtering for time series.")

def create_component_failure_frequency_chart(df_input):
    component_cols = ['Diagnose code 1-Category', 'Diagnose code 2-Category', 'Diagnose code 3-Category']
    existing_cols = [col for col in df_input.columns if col in component_cols]
    if not existing_cols or df_input.empty or all(df_input[col].dropna().empty for col in existing_cols):
        return get_empty_figure_with_message("No component data to display.")
    components_list = []
    for col in existing_cols:
        components_list.append(df_input[col])
    if components_list:
        components = pd.concat(components_list).dropna()
        if not components.empty:
            component_counts = components.value_counts().reset_index()
            component_counts.columns = ['Component', 'Count']
            fig = px.pie(component_counts, values='Count', names='Component', title='Diagnosis Code Distribution',
                                     color_discrete_sequence=QUALITATIVE_COLOR_SEQUENCE)
            fig.update_layout(
                plot_bgcolor=card_bg_color, paper_bgcolor=card_bg_color,
                font=dict(family=body_font_family, color=card_body_text_color, size=12),
                margin=dict(t=50, b=50, l=40, r=40),
                title_font=dict(size=20, color=card_text_color, family=headline_font_family), title_x=0.5,
                legend_title_text='Component',
                legend=dict(font=dict(color=card_body_text_color)),
                transition_duration=1000
            )
            return fig
    return get_empty_figure_with_message("No component data after filtering.")

def create_product_family_analysis_chart(df_unique_srs_input, selected_p_type='All', selected_s_status='All', selected_a_cat='All', selected_pf_group='All'):
    temp_df = df_unique_srs_input.copy()

    if selected_p_type and selected_p_type != 'All' and 'SR Problem type' in temp_df.columns:
        if isinstance(selected_p_type, list):
            temp_df = temp_df[temp_df['SR Problem type'].isin(selected_p_type)]
        else:
            temp_df = temp_df[temp_df['SR Problem type'] == selected_p_type]

    if selected_s_status and selected_s_status != 'All' and 'Simplified SR Status' in temp_df.columns:
        if isinstance(selected_s_status, list):
            temp_df = temp_df[temp_df['Simplified SR Status'].isin(selected_s_status)]
        else:
            temp_df = temp_df[temp_df['Simplified SR Status'] == selected_s_status]

    if selected_a_cat and selected_a_cat != 'All' and 'Ageing Category' in temp_df.columns:
        if isinstance(selected_a_cat, list):
            temp_df = temp_df[temp_df['Ageing Category'].isin(selected_a_cat)]
        else:
            temp_df = temp_df[temp_df['Ageing Category'] == selected_a_cat]

    chart_title = ""
    x_axis_column = ""

    if selected_pf_group and selected_pf_group != 'All':
        if 'Product Family Group' not in temp_df.columns or 'Product family' not in temp_df.columns:
            return get_empty_figure_with_message(f"Missing 'Product Family Group' or 'Product family' column for drill-down analysis.")

        if isinstance(selected_pf_group, list):
            temp_df_filtered_by_group = temp_df[temp_df['Product Family Group'].isin(selected_pf_group)].copy()
            x_axis_title_display = f"Individual Meters ({', '.join(selected_pf_group)})"
            chart_title = f"Meters in '{', '.join(selected_pf_group)}' Families"
        else:
            temp_df_filtered_by_group = temp_df[temp_df['Product Family Group'] == selected_pf_group].copy()
            x_axis_title_display = f"Individual Meters ({selected_pf_group})"
            chart_title = f"Meters in '{selected_pf_group}' Family"

        x_axis_column = 'Product family'

        if temp_df_filtered_by_group.empty or temp_df_filtered_by_group[x_axis_column].dropna().empty:
            return get_empty_figure_with_message(f"No individual meter data found for selected Product Families under current filters.")
        counts_df = temp_df_filtered_by_group[x_axis_column].value_counts().reset_index()
        counts_df.columns = [x_axis_column, 'Count']
    else:
        x_axis_column = 'Product Family Group'
        chart_title = "SR Count by Product Group"
        x_axis_title_display = "Product Family Group"

        if x_axis_column not in temp_df.columns or temp_df.empty or temp_df[x_axis_column].dropna().empty:
            return get_empty_figure_with_message(f"No {x_axis_title_display} data for selected filters.")
        counts_df = temp_df[x_axis_column].value_counts().reset_index()
        counts_df.columns = [x_axis_column, 'Count']

    fig = px.bar(counts_df, x=x_axis_column, y='Count', color_discrete_sequence=SINGLE_COLOR_SEQUENCE)
    fig.update_layout(
        title=chart_title,
        plot_bgcolor=card_bg_color, paper_bgcolor=card_bg_color,
        font=dict(family=body_font_family, color=card_body_text_color, size=12),
        margin=dict(t=50, b=50, l=40, r=40),
        title_font=dict(size=20, color=card_text_color, family=headline_font_family), title_x=0.5,
        xaxis_title=x_axis_title_display, yaxis_title="Count of Service Requests",
        yaxis=dict(title="Count", tickformat=".2s", title_font=dict(color=card_text_color)),
        xaxis=dict(title_font=dict(color=card_text_color)),
        transition_duration=1000
    )
    return fig

def create_stage_wise_sr_tat_chart(df_input, selected_p_type_tat='All', selected_pf_tat='All'):
    required_tat_cat_cols = ['M1_TAT_Category', 'M2_TAT_Category', 'M3_TAT_Category', 'SR Problem type', 'Product Family Group', 'SR no.']

    if not all(col in df_input.columns for col in required_tat_cat_cols) or df_input.empty:
        return get_empty_figure_with_message("Missing TAT category columns or empty data for TAT analysis.")

    df_tat = df_input.copy()

    # THIS FUNCTION NOW EXPECTS THE FULL DATA, SO WE DE-DUPLICATE INTERNALLY
    if 'SR no.' in df_tat.columns:
        df_tat = df_tat.drop_duplicates(subset=['SR no.']).copy()

    tat_labels_ordered = ['<7D', '7-10D', '11-15D', '>15D']
    current_df_filtered = df_tat.copy()

    if selected_p_type_tat and selected_p_type_tat != 'All' and 'SR Problem type' in current_df_filtered.columns:
        if isinstance(selected_p_type_tat, list):
            current_df_filtered = current_df_filtered[current_df_filtered['SR Problem type'].isin(selected_p_type_tat)]
        else:
            current_df_filtered = current_df_filtered[current_df_filtered['SR Problem type'] == selected_p_type_tat]

    if selected_pf_tat and selected_pf_tat != 'All' and 'Product Family Group' in current_df_filtered.columns:
        if isinstance(selected_pf_tat, list):
            current_df_filtered = current_df_filtered[current_df_filtered['Product Family Group'].isin(selected_pf_tat)]
        else:
            current_df_filtered = current_df_filtered[current_df_filtered['Product Family Group'] == selected_pf_tat]

    if current_df_filtered.empty:
        p_type_display = ', '.join(selected_p_type_tat) if isinstance(selected_p_type_tat, list) else selected_p_type_tat
        pf_group_display = ', '.join(selected_pf_tat) if isinstance(selected_pf_tat, list) else selected_pf_tat
        return get_empty_figure_with_message(f"No TAT data for selected Problem Type(s): {p_type_display} and Product Family Group(s): {pf_group_display}.")

    fig = go.Figure()
    x_stages = ['M1', 'M2', 'M3']
    stage_col_names = ['M1_TAT_Category', 'M2_TAT_Category', 'M3_TAT_Category']

    for tat_cat_label in tat_labels_ordered:
        y_counts = []
        list_of_sr_numbers_for_traces = []
        for i, stage_col_name in enumerate(stage_col_names):
            stage_tat_filtered_df_segment = current_df_filtered[current_df_filtered[stage_col_name] == tat_cat_label]
            count_in_stage = len(stage_tat_filtered_df_segment)
            y_counts.append(count_in_stage)
            sr_numbers_for_this_segment = stage_tat_filtered_df_segment['SR no.'].dropna().unique().tolist()
            list_of_sr_numbers_for_traces.append(sr_numbers_for_this_segment)

        fig.add_trace(go.Bar(
            x=x_stages, y=y_counts, name=tat_cat_label,
            marker_color=STAGE_TAT_COLORS.get(tat_cat_label, '#888888'),
            legendgroup=tat_cat_label, showlegend=True, opacity=0.95,
            hovertemplate='<b>Stage:</b> %{x}<br><b>TAT Category:</b> %{fullData.name}<br><b>Count:</b> %{y}<extra></extra>',
            customdata=list_of_sr_numbers_for_traces
        ))

    p_type_title_part = f"Problem Type: {', '.join(selected_p_type_tat)}" if isinstance(selected_p_type_tat, list) and selected_p_type_tat != 'All' else f"Problem Type: {selected_p_type_tat}"
    pf_group_title_part = f"Product Family Group: {', '.join(selected_pf_tat)}" if isinstance(selected_pf_tat, list) and selected_pf_tat != 'All' else f"Product Family Group: {selected_pf_tat}"

    fig.update_layout(
        barmode='stack',
        title_text=f"Stage wise SR TAT ({p_type_title_part}, {pf_group_title_part})",
        xaxis_title="Stage", yaxis_title="Count of Unique Service Requests",
        plot_bgcolor=card_bg_color, paper_bgcolor=card_bg_color,
        font=dict(family=body_font_family, color=card_body_text_color, size=12),
        margin=dict(t=150, b=50, l=60, r=40),
        title_font=dict(size=20, color=card_text_color, family=headline_font_family), title_x=0.5,
        legend_title_text='TAT Category',
        legend=dict(
            orientation="h", yanchor="bottom", y=1.05, xanchor="right", x=1,
            bgcolor=card_bg_color, bordercolor="#e5e7eb", borderwidth=1,
            font=dict(color=card_body_text_color)
        ),
        yaxis=dict(tickformat=".2s", showgrid=True, gridwidth=1, gridcolor='#e0e0e0', title_font=dict(color=card_text_color)),
        xaxis=dict(showgrid=False, title_font=dict(color=card_text_color)),
        transition_duration=1000
    )
    fig.update_traces(marker_line_width=0.5, marker_line_color='rgba(0,0,0,0.15)')
    return fig

def create_month_wise_sr_tat_chart(df_input):
    required_cols = ['SR creation date', 'SR no.', 'Overall_TAT_Category', 'MonthYear', 'Simplified SR Status']
    if not all(col in df_input.columns for col in required_cols) or df_input.empty:
        return get_empty_figure_with_message("Missing required columns for Month wise SR TAT.")

    df_plot = df_input.copy()

    # THIS FUNCTION NOW EXPECTS THE FULL DATA, SO WE DE-DUPLICATE INTERNALLY
    if 'SR no.' in df_plot.columns:
        # Keep the last entry for each SR to get its most recent status
        df_plot = df_plot.sort_values('SR creation date', ascending=True).drop_duplicates(subset=['SR no.'], keep='last')

    df_plot['SR creation date'] = pd.to_datetime(df_plot['SR creation date'], errors='coerce').dt.normalize()
    df_plot.dropna(subset=['SR creation date', 'MonthYear', 'Overall_TAT_Category', 'Simplified SR Status'], inplace=True)
    if df_plot.empty:
        return get_empty_figure_with_message("No valid data for Month wise SR TAT after initial cleaning.")

    all_month_years = df_plot['MonthYear'].dropna().unique()
    if len(all_month_years) > 0:
        temp_sort_df = pd.DataFrame({'MonthYear': all_month_years})
        temp_sort_df['SortKey'] = pd.to_datetime(temp_sort_df['MonthYear'], format='%b-%y')
        sorted_months = temp_sort_df.sort_values('SortKey')['MonthYear'].tolist()
    else:
        sorted_months = []

    tat_category_order = ['<=21D', '21-30 D', '>30D', 'OPEN', 'N/A Status/Date']

    grouped_data = df_plot.groupby(['MonthYear', 'Overall_TAT_Category']).size().reset_index(name='Count')
    grouped_data['MonthYear'] = pd.Categorical(grouped_data['MonthYear'], categories=sorted_months, ordered=True)
    grouped_data['Overall_TAT_Category'] = pd.Categorical(grouped_data['Overall_TAT_Category'], categories=tat_category_order, ordered=True)
    grouped_data = grouped_data.sort_values(by=['MonthYear', 'Overall_TAT_Category'])
    if grouped_data.empty:
        return get_empty_figure_with_message("No data after final grouping for Month wise SR TAT.")
    fig = go.Figure()
    for category in tat_category_order:
        category_data = grouped_data[grouped_data['Overall_TAT_Category'] == category]
        full_x_axis_df = pd.DataFrame({'MonthYear': sorted_months})
        category_data_for_plot = pd.merge(full_x_axis_df, category_data, on='MonthYear', how='left').fillna({'Count': 0})
        category_data_for_plot['MonthYear'] = pd.Categorical(category_data_for_plot['MonthYear'], categories=sorted_months, ordered=True)
        category_data_for_plot = category_data_for_plot.sort_values(by='MonthYear')
        sr_numbers_for_this_category_by_month = []
        for month in category_data_for_plot['MonthYear']:
            month_cat_filtered_df_segment = df_plot[
                (df_plot['MonthYear'] == month) &
                (df_plot['Overall_TAT_Category'] == category)
            ]
            sr_numbers_for_this_segment = month_cat_filtered_df_segment['SR no.'].dropna().unique().tolist()
            sr_numbers_for_this_category_by_month.append(sr_numbers_for_this_segment)
        fig.add_trace(go.Bar(
            x=category_data_for_plot['MonthYear'], y=category_data_for_plot['Count'], name=category,
            marker_color=OVERALL_TAT_COLORS.get(category, '#2563eb'),
            hovertemplate='<b>Month:</b> %{x}<br><b>Category:</b> %{fullData.name}<br><b>Count:</b> %{y}<extra></extra>',
            customdata=sr_numbers_for_this_category_by_month
        ))
    fig.update_layout(
        barmode='stack', title_text="Month wise SR TAT",
        xaxis_title="Month", yaxis_title="Count of Service Requests",
        plot_bgcolor=card_bg_color, paper_bgcolor=card_bg_color,
        font=dict(family=body_font_family, color=card_body_text_color, size=12),
        margin=dict(t=80, b=100, l=60, r=40),
        title_font=dict(size=20, color=card_text_color, family=headline_font_family), title_x=0.5,
        legend_title_text='TAT Category',
        legend=dict(
            orientation="h", yanchor="bottom", y=-0.25, xanchor="center", x=0.5,
            bgcolor=card_bg_color, bordercolor="#e5e7eb", borderwidth=1,
            font=dict(color=card_body_text_color)
        ),
        yaxis=dict(tickformat=".2s", showgrid=True, gridwidth=1, gridcolor='#e0e0e0', title_font=dict(color=card_text_color)),
        xaxis=dict(tickangle=0, showgrid=False, type='category', categoryorder='array', categoryarray=sorted_months, title_font=dict(color=card_text_color)),
        transition_duration=1000
    )
    fig.update_traces(marker_line_width=0.5, marker_line_color='rgba(0,0,0,0.15)')
    return fig


# --- Dash App Layout Definition ---
app.layout = html.Div(style={'fontFamily': body_font_family, 'backgroundColor': bg_color, 'minHeight': '100vh', 'padding': '20px'}, children=[
    html.Img(
        src=app.get_asset_url('secure_logo_transparent.png'),
        alt="SECURE Logo",
        style={
            'display': 'block',
            'margin': '0 auto 30px auto',
            'height': '100px',
            'width': 'auto',
            'objectFit': 'contain'
        }
    ),

    dcc.Store(id='stored-data', data=process_df_to_json(get_empty_dataframe_structure().copy())),
    dcc.Store(id='graph-click-data-output', data={}),
    dcc.Store(id='stored-graph-figures', data={}),
    dcc.Download(id="download-pdf-data"),

    dcc.Interval(
        id='interval-component',
        interval=15*1000,
        n_intervals=0
    ),

    dcc.Interval(
        id='initial-animation-trigger',
        interval=200,
        max_intervals=1
    ),

    dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle("Full Screen View", style={'color': card_text_color, 'fontFamily': headline_font_family})),
            dbc.ModalBody(
                dcc.Graph(
                    id='fullscreen-graph-display',
                    config={'displayModeBar': True, 'responsive': True, 'useResizeHandler': True},
                    style={'height': '100%', 'width': '100%'}
                ),
                style={'padding': '0', 'flexGrow': '1', 'display': 'flex', 'flexDirection': 'column'}
            ),
        ],
        id="fullscreen-modal",
        fullscreen=True,
        is_open=False,
        scrollable=True,
        className="modal-lg"
    ),


    dbc.Container(fluid=True, style={'maxWidth': max_container_width, 'padding': '0'}, children=[
        dbc.Row(className="mb-4", children=[
            dbc.Col(width=12, children=[
                dbc.Card(style={'backgroundColor': card_bg_color, 'borderRadius': card_border_radius, 'boxShadow': box_shadow_css, 'padding': '1.5rem'}, children=[
                    html.H3("Global Filters (Dashboard-Specific)", style={'color': card_text_color, 'marginTop': '0', 'marginBottom': '20px', 'fontFamily': headline_font_family, 'fontWeight': '600'}),
                    dbc.Row(className="g-3", children=[
                        dbc.Col(lg=3, md=6, children=[
                            html.Label("SR Problem Type:", style={'color': card_body_text_color, 'marginBottom': '5px', 'display': 'block'}),
                            dcc.Dropdown(
                                id='problem-type-filter', options=[], value='All', clearable=False, multi=True,
                                style={'fontFamily': body_font_family, 'backgroundColor': card_bg_color, 'borderColor': '#e5e7eb', 'color': card_text_color},
                                className='dbc'
                            )
                        ]),
                        dbc.Col(lg=3, md=6, children=[
                            html.Label("SR Status:", style={'color': card_body_text_color, 'marginBottom': '5px', 'display': 'block'}),
                            dcc.Dropdown(
                                id='status-filter', options=[], value='All', clearable=False, multi=True,
                                style={'fontFamily': body_font_family, 'backgroundColor': card_bg_color, 'borderColor': '#e5e7eb', 'color': card_text_color},
                                className='dbc'
                            )
                        ]),
                        dbc.Col(lg=3, md=6, children=[
                            html.Label("Ageing Category:", style={'color': card_body_text_color, 'marginBottom': '5px', 'display': 'block'}),
                            dcc.Dropdown(
                                id='ageing-category-filter', options=[], value='All', clearable=False, multi=True,
                                style={'fontFamily': body_font_family, 'backgroundColor': card_bg_color, 'borderColor': '#e5e7eb', 'color': card_text_color},
                                className='dbc'
                            )
                        ]),
                        dbc.Col(lg=3, md=6, children=[
                            html.Label("Product Family Group:", style={'color': card_body_text_color, 'marginBottom': '5px', 'display': 'block'}),
                            dcc.Dropdown(
                                id='product-family-filter', options=[], value='All', clearable=False, multi=True,
                                style={'fontFamily': body_font_family, 'backgroundColor': card_bg_color, 'borderColor': '#e5e7eb', 'color': card_text_color},
                                className='dbc'
                            )
                        ]),
                    ]),
                    dbc.Row(className="g-3 mt-3", children=[
                        # MODIFIED: Changed lg width to 6 to make space for the new dropdown
                        dbc.Col(lg=6, md=12, children=[
                            html.Label("SR Creation Date Range (ignored for weekly charts):", style={'color': card_body_text_color, 'marginBottom': '5px', 'display': 'block'}),
                            dcc.DatePickerRange(
                                id='global-date-picker-range', start_date_placeholder_text="Start Date",
                                end_date_placeholder_text="End Date", calendar_orientation='vertical',
                                display_format='DD-MMM-YYYY',
                                style={'width': '100%', 'backgroundColor': card_bg_color, 'borderColor': '#e5e7eb', 'borderRadius': '6px', 'borderWidth': '1px'},
                                className='dbc'
                            )
                        ]),
                        # NEW: Dropdown for week selection
                        dbc.Col(lg=3, md=6, children=[
                            html.Label("Select a Week:", style={'color': card_body_text_color, 'marginBottom': '5px', 'display': 'block'}),
                            dcc.Dropdown(
                                id='week-selector-dropdown',
                                options=[],
                                value=current_week_value, # MODIFIED: Set default value
                                clearable=True,
                                placeholder="For daily performance chart...",
                                style={'fontFamily': body_font_family, 'backgroundColor': card_bg_color, 'borderColor': '#e5e7eb', 'color': card_text_color},
                                className='dbc'
                            )
                        ]),
                        dbc.Col(lg=3, md=6, className="d-flex align-items-end", children=[
                            html.Button(
                                'Reset Dates', id='reset-date-filter-button', n_clicks=0,
                                style={
                                    'backgroundColor': '#e5e7eb', 'color': card_text_color,
                                    'border': '1px solid #d1d5db', 'borderRadius': '6px',
                                    'padding': '8px 12px', 'cursor': 'pointer', 'width': '100%'
                                }
                            )
                        ])
                    ]),
                    html.Div(className="mt-4", children=[
                        html.Button(
                            [html.I(className="bi bi-file-earmark-pdf me-2"), "Download Dashboard as PDF"],
                            id='download-pdf-button', n_clicks=0,
                            style={
                                'backgroundColor': '#DC3545', 'color': 'white', 'border': 'none',
                                'borderRadius': '6px', 'padding': '10px 20px', 'cursor': 'pointer',
                                'fontSize': '1em', 'width': '100%'
                            }
                        )
                    ])
                ])
            ])
        ]),

        # NEW: Row for Daily Performance Chart
        dbc.Row(className="mb-4", children=[
            dbc.Col(width=12, children=[
                dbc.Card(style={'backgroundColor': card_bg_color, 'borderRadius': card_border_radius, 'boxShadow': box_shadow_css}, children=[
                    dbc.CardHeader(style={'display': 'flex', 'justifyContent': 'flex-end', 'padding': '10px 15px', 'borderBottom': 'none'}, children=[
                        html.Button(
                            html.I(className="bi bi-arrows-fullscreen"),
                            id={'type': 'maximize-button', 'index': 'daily-performance-chart'},
                            n_clicks=0,
                            style={
                                'backgroundColor': 'transparent', 'border': 'none', 'color': card_body_text_color,
                                'fontSize': '1.2em', 'cursor': 'pointer', 'padding': '5px'
                            }
                        )
                    ]),
                    dbc.CardBody(children=[
                        dcc.Graph(id='daily-performance-chart', config={'displayModeBar': False})
                    ])
                ])
            ])
        ]),

        dbc.Row(className="mb-4", children=[
            dbc.Col(width=12, children=[
                dbc.Card(style={'backgroundColor': card_bg_color, 'borderRadius': card_border_radius, 'boxShadow': box_shadow_css}, children=[
                    dbc.CardHeader(style={'display': 'flex', 'justifyContent': 'flex-end', 'padding': '10px 15px', 'borderBottom': 'none'}, children=[
                        html.Button(
                            html.I(className="bi bi-arrows-fullscreen"),
                            id={'type': 'maximize-button', 'index': 'weekly-funnel-chart'},
                            n_clicks=0,
                            style={
                                'backgroundColor': 'transparent', 'border': 'none', 'color': card_body_text_color,
                                'fontSize': '1.2em', 'cursor': 'pointer', 'padding': '5px'
                            }
                        )
                    ]),
                    dbc.CardBody(children=[
                        dcc.Graph(id='weekly-funnel-chart', config={'displayModeBar': False})
                    ])
                ])
            ])
        ]),

        dbc.Row(className="mb-4", children=[
            dbc.Col(width=12, children=[
                dbc.Card(style={'backgroundColor': card_bg_color, 'borderRadius': card_border_radius, 'boxShadow': box_shadow_css}, children=[
                    dbc.CardHeader(style={'display': 'flex', 'justifyContent': 'flex-end', 'padding': '10px 15px', 'borderBottom': 'none'}, children=[
                        html.Button(
                            html.I(className="bi bi-arrows-fullscreen"),
                            id={'type': 'maximize-button', 'index': 'month-wise-sr-tat-chart'},
                            n_clicks=0,
                            style={
                                'backgroundColor': 'transparent', 'border': 'none', 'color': card_body_text_color,
                                'fontSize': '1.2em', 'cursor': 'pointer', 'padding': '5px'
                            }
                        )
                    ]),
                    dbc.CardBody(children=[
                        dcc.Graph(id='month-wise-sr-tat-chart', config={'displayModeBar': False})
                    ])
                ])
            ])
        ]),

        dbc.Row(className="mb-4", children=[
            dbc.Col(lg=6, md=12, className="mb-4 mb-lg-0", children=[
                dbc.Card(style={'backgroundColor': card_bg_color, 'borderRadius': card_border_radius, 'boxShadow': box_shadow_css}, children=[
                    dbc.CardHeader(style={'display': 'flex', 'justifyContent': 'flex-end', 'padding': '10px 15px', 'borderBottom': 'none'}, children=[
                        html.Button(
                            html.I(className="bi bi-arrows-fullscreen"),
                            id={'type': 'maximize-button', 'index': 'tat-chart'},
                            n_clicks=0,
                            style={
                                'backgroundColor': 'transparent', 'border': 'none', 'color': card_body_text_color,
                                'fontSize': '1.2em', 'cursor': 'pointer', 'padding': '5px'
                            }
                        )
                    ]),
                    dbc.CardBody(children=[
                        dcc.Graph(id='tat-chart', config={'displayModeBar': False}, style={'height': '500px'}),
                        html.Div(style={'fontSize': '0.9em', 'color': card_body_text_color, 'marginTop': '15px', 'paddingLeft': '10px'}, children=[
                            html.P([html.B("M1: "), "SR Creation Date to GRN Date (Time until meter received at repair center)."], style={'marginBottom': '5px'}),
                            html.P([html.B("M2: "), "GRN Date to Inter-org Challan Date (Time spent at repair center)."], style={'marginBottom': '5px'}),
                            html.P([html.B("M3: "), "Inter-org Challan Date to SR Closure Date (Time from dispatch to final SR closure)."])
                        ])
                    ])
                ])
            ]),
            dbc.Col(lg=6, md=12, children=[
                dbc.Card(style={'backgroundColor': card_bg_color, 'borderRadius': card_border_radius, 'boxShadow': box_shadow_css}, children=[
                    dbc.CardHeader(style={'display': 'flex', 'justifyContent': 'flex-end', 'padding': '10px 15px', 'borderBottom': 'none'}, children=[
                        html.Button(
                            html.I(className="bi bi-arrows-fullscreen"),
                            id={'type': 'maximize-button', 'index': 'product-family-chart'},
                            n_clicks=0,
                            style={
                                'backgroundColor': 'transparent', 'border': 'none', 'color': card_body_text_color,
                                'fontSize': '1.2em', 'cursor': 'pointer', 'padding': '5px'
                            }
                        )
                    ]),
                    dbc.CardBody(children=[
                        dcc.Graph(id='product-family-chart', config={'displayModeBar': False}, style={'height': '500px'}),
                        html.Div(style={'minHeight': '75px'})
                    ])
                ])
            ])
        ]),

        dbc.Row(className="mb-4", children=[
            dbc.Col(lg=6, md=12, className="mb-4 mb-lg-0", children=[
                dbc.Card(style={'backgroundColor': card_bg_color, 'borderRadius': card_border_radius, 'boxShadow': box_shadow_css}, children=[
                    dbc.CardHeader(style={'display': 'flex', 'justifyContent': 'flex-end', 'padding': '10px 15px', 'borderBottom': 'none'}, children=[
                        html.Button(
                            html.I(className="bi bi-arrows-fullscreen"),
                            id={'type': 'maximize-button', 'index': 'branch-srs-chart'},
                            n_clicks=0,
                            style={
                                'backgroundColor': 'transparent', 'border': 'none', 'color': card_body_text_color,
                                'fontSize': '1.2em', 'cursor': 'pointer', 'padding': '5px'
                            }
                        )
                    ]),
                    dbc.CardBody(children=[
                        dcc.Graph(id='branch-srs-chart', config={'displayModeBar': False})
                    ])
                ])
            ]),
            dbc.Col(lg=6, md=12, children=[
                dbc.Card(style={'backgroundColor': card_bg_color, 'borderRadius': card_border_radius, 'boxShadow': box_shadow_css}, children=[
                    dbc.CardHeader(style={'display': 'flex', 'justifyContent': 'flex-end', 'padding': '10px 15px', 'borderBottom': 'none'}, children=[
                        html.Button(
                            html.I(className="bi bi-arrows-fullscreen"),
                            id={'type': 'maximize-button', 'index': 'status-distribution-chart'},
                            n_clicks=0,
                            style={
                                'backgroundColor': 'transparent', 'border': 'none', 'color': card_body_text_color,
                                'fontSize': '1.2em', 'cursor': 'pointer', 'padding': '5px'
                            }
                        )
                    ]),
                    dbc.CardBody(children=[
                        dcc.Graph(id='status-distribution-chart', config={'displayModeBar': False})
                    ])
                ])
            ])
        ]),

        dbc.Row(className="mb-4", children=[
            dbc.Col(lg=6, md=12, className="mb-4 mb-lg-0", children=[
                dbc.Card(style={'backgroundColor': card_bg_color, 'borderRadius': card_border_radius, 'boxShadow': box_shadow_css}, children=[
                    dbc.CardHeader(style={'display': 'flex', 'justifyContent': 'flex-end', 'padding': '10px 15px', 'borderBottom': 'none'}, children=[
                        html.Button(
                            html.I(className="bi bi-arrows-fullscreen"),
                            id={'type': 'maximize-button', 'index': 'srs-over-time-chart'},
                            n_clicks=0,
                            style={
                                'backgroundColor': 'transparent', 'border': 'none', 'color': card_body_text_color,
                                'fontSize': '1.2em', 'cursor': 'pointer', 'padding': '5px'
                            }
                        )
                    ]),
                    dbc.CardBody(children=[
                        dcc.Graph(id='srs-over-time-chart', config={'displayModeBar': False})
                    ])
                ])
            ]),
            dbc.Col(lg=6, md=12, children=[
                dbc.Card(style={'backgroundColor': card_bg_color, 'borderRadius': card_border_radius, 'boxShadow': box_shadow_css}, children=[
                    dbc.CardHeader(style={'display': 'flex', 'justifyContent': 'flex-end', 'padding': '10px 15px', 'borderBottom': 'none'}, children=[
                        html.Button(
                            html.I(className="bi bi-arrows-fullscreen"),
                            id={'type': 'maximize-button', 'index': 'component-failure-chart'},
                            n_clicks=0,
                            style={
                                'backgroundColor': 'transparent', 'border': 'none', 'color': card_body_text_color,
                                'fontSize': '1.2em', 'cursor': 'pointer', 'padding': '5px'
                            }
                        )
                    ]),
                    dbc.CardBody(children=[
                        dcc.Graph(id='component-failure-chart', config={'displayModeBar': False})
                    ])
                ])
            ])
        ]),

        dbc.Row(className="mb-4", children=[
            dbc.Col(width=12, children=[
                dbc.Card(style={'backgroundColor': card_bg_color, 'borderRadius': card_border_radius, 'boxShadow': box_shadow_css}, children=[
                    dbc.CardHeader(style={'display': 'flex', 'justifyContent': 'space-between', 'alignItems': 'center', 'padding': '10px 15px', 'borderBottom': 'none'}, children=[
                        html.H3("SR Status by Ageing Category", style={'color': card_text_color, 'marginTop': '0', 'marginBottom': '0', 'fontFamily': headline_font_family, 'fontWeight': '600'}),
                        html.Button(
                            html.I(className="bi bi-arrows-fullscreen"),
                            id={'type': 'maximize-button', 'index': 'ageing-status-chart'},
                            n_clicks=0,
                            style={
                                'backgroundColor': 'transparent', 'border': 'none', 'color': card_body_text_color,
                                'fontSize': '1.2em', 'cursor': 'pointer', 'padding': '5px'
                            }
                        )
                    ]),
                    html.Div(style={'display': 'flex', 'alignItems': 'center', 'gap': '10px', 'marginBottom': '20px'}, children=[
                        html.Label("Select Ageing Category:", style={'color': card_body_text_color, 'minWidth': '150px'}),
                        dcc.Dropdown(
                            id='ageing-category-chart-filter',
                            options=[
                                {'label': 'All', 'value': 'All'},
                                {'label': '0-21 days', 'value': '0-21 days'},
                                {'label': '22-30 days', 'value': '22-30 days'},
                                {'label': '31-45 days', 'value': '31-45 days'},
                                {'label': '46-90 days', 'value': '46-90 days'},
                                {'label': '90+ days', 'value': '90+ days'}
                            ],
                            value='All', clearable=False, multi=True,
                            style={'flexGrow': 1, 'fontFamily': body_font_family, 'backgroundColor': card_bg_color, 'borderColor': '#e5e7eb', 'color': card_text_color},
                            className='dbc'
                        )
                    ]),
                    dbc.CardBody(children=[
                        dcc.Graph(id='ageing-status-chart', config={'displayModeBar': False})
                    ])
                ])
            ])
        ]),
    ])
])


# --- Callbacks ---
shared_data_proxy_global = None
click_data_queue_global = None

@app.callback(
    Output('stored-data', 'data'),
    Input('interval-component', 'n_intervals')
)
def update_store_data(n):
    global shared_data_proxy_global
    if shared_data_proxy_global:
        current_json_data = shared_data_proxy_global.get('live_data', process_df_to_json(get_empty_dataframe_structure().copy()))
        return current_json_data
    return dash.no_update

# THIS IS THE MODIFIED CALLBACK
@app.callback(
    Output('graph-click-data-output', 'data'),
    [Input('tat-chart', 'clickData'),
     Input('month-wise-sr-tat-chart', 'clickData'),
     Input('ageing-status-chart', 'clickData')],
    [State('tat-chart', 'figure'),
     State('month-wise-sr-tat-chart', 'figure'),
     State('ageing-status-chart', 'figure')],
    prevent_initial_call=True
)
def handle_graph_click(tat_click_data, month_tat_click_data, ageing_click_data,
                       tat_fig, month_tat_fig, ageing_fig):
    ctx = dash.callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate

    triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
    click_data = ctx.triggered[0]['value']

    if not click_data or not click_data.get('points'):
        raise dash.exceptions.PreventUpdate

    point = click_data['points'][0]
    clicked_sr_numbers = []
    clicked_info = {}

    figure_map = {
        'tat-chart': tat_fig,
        'month-wise-sr-tat-chart': month_tat_fig,
        'ageing-status-chart': ageing_fig
    }
    figure = figure_map.get(triggered_id)

    if not figure:
        raise dash.exceptions.PreventUpdate

    curve_number = point['curveNumber']
    trace_data = figure['data'][curve_number]

    if triggered_id in ['tat-chart', 'month-wise-sr-tat-chart']:
        # These charts have one trace per category, with multiple points (bars)
        point_index = point['pointIndex']
        if 'customdata' in trace_data and len(trace_data.get('customdata', [])) > point_index:
            clicked_sr_numbers = trace_data['customdata'][point_index]

        category_name = trace_data.get('name')
        x_val = point.get('x')
        if triggered_id == 'tat-chart':
            clicked_info = {'chart': 'TAT', 'stage': x_val, 'tat_category': category_name}
        else:
            clicked_info = {'chart': 'Month TAT', 'month': x_val, 'tat_category': category_name}

    elif triggered_id == 'ageing-status-chart':
        # This chart has one trace per bar segment
        if 'customdata' in trace_data and trace_data['customdata'] and isinstance(trace_data['customdata'][0], list):
            clicked_sr_numbers = trace_data['customdata'][0]
        clicked_info = {'chart': 'Ageing Status', 'status': point.get('x'), 'ageing_category': trace_data.get('name')}

    if clicked_sr_numbers:
        print(f"Graph Clicked: {triggered_id}, Info: {clicked_info}, SRs Count: {len(clicked_sr_numbers)}")
        global click_data_queue_global
        if click_data_queue_global:
            data_to_send = {'sr_numbers': clicked_sr_numbers, 'chart_info': clicked_info}
            click_data_queue_global.put(data_to_send)
            return {'sr_numbers_sent_via_queue': len(clicked_sr_numbers), 'timestamp': datetime.now().isoformat()}
        else:
            print("Warning: click_data_queue_global not set, cannot send clicked SRs to Tkinter.")

    raise dash.exceptions.PreventUpdate


@app.callback(
    Output('problem-type-filter', 'options'),
    Output('status-filter', 'options'),
    Output('ageing-category-filter', 'options'),
    Output('product-family-filter', 'options'),
    Output('week-selector-dropdown', 'options'), # NEW OUTPUT
    Input('stored-data', 'data')
)
def populate_dropdown_options(data_json):
    df = read_df_from_json(data_json)

    problem_type_options = [{'label': 'All', 'value': 'All'}]
    if 'SR Problem type' in df.columns and not df['SR Problem type'].dropna().empty:
        problem_type_options.extend([{'label': i, 'value': i} for i in sorted(df['SR Problem type'].dropna().unique())])

    status_options = [{'label': 'All', 'value': 'All'}]
    if 'Simplified SR Status' in df.columns and not df['Simplified SR Status'].dropna().empty:
        unique_simplified_statuses = df['Simplified SR Status'].dropna().astype(str).unique()
        sorted_statuses = sorted(unique_simplified_statuses, key=lambda x: (x != 'Open', x != 'Closed', x != 'N/A', x))
        status_options.extend([{'label': i, 'value': i} for i in sorted_statuses if i != 'N/A'])

    ageing_labels = ['0-21 days', '22-30 days', '31-45 days', '46-90 days', '90+ days']
    ageing_category_options = [{'label': 'All', 'value': 'All'}]
    ageing_category_options.extend([{'label': label, 'value': label} for label in ageing_labels])

    product_family_group_options = [{'label': 'All', 'value': 'All'}]
    if 'Product Family Group' in df.columns and not df['Product Family Group'].dropna().empty:
        product_family_group_options.extend([{'label': i, 'value': i} for i in sorted(df['Product Family Group'].dropna().unique())])

    # NEW: Logic to populate the week selector dropdown
    week_options = []
    if 'GRN date' in df.columns and not df['GRN date'].dropna().empty:
        df_weeks = df.dropna(subset=['GRN date']).copy()
        df_weeks['Week'] = df_weeks['GRN date'].dt.strftime('%Y-W%U')
        unique_weeks = sorted(df_weeks['Week'].unique(), reverse=True)
        week_options = [{'label': f'Week of {pd.to_datetime(w + "-0", format="%Y-W%U-%w").strftime("%b %d, %Y")}', 'value': w} for w in unique_weeks]


    return problem_type_options, status_options, ageing_category_options, product_family_group_options, week_options

@app.callback(
    Output('global-date-picker-range', 'start_date'),
    Output('global-date-picker-range', 'end_date'),
    Input('reset-date-filter-button', 'n_clicks'),
    prevent_initial_call=True
)
def reset_date_filters(n_clicks):
    if n_clicks > 0:
        return None, None
    return dash.no_update, dash.no_update

@app.callback(
    Output('fullscreen-modal', 'is_open'),
    Output('fullscreen-graph-display', 'figure'),
    Output('fullscreen-graph-display', 'config'),
    Output('fullscreen-modal', 'title'),
    Input({'type': 'maximize-button', 'index': ALL}, 'n_clicks'),
    State('stored-graph-figures', 'data'),
    State('fullscreen-modal', 'is_open'),
    prevent_initial_call=True
)
def toggle_fullscreen_modal(n_clicks_list, stored_figures, is_modal_open):
    ctx = dash.callback_context
    if not ctx.triggered:
        raise dash.exceptions.PreventUpdate
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    if 'maximize-button' in button_id:
        clicked_button_info = eval(button_id)
        graph_id_to_maximize = clicked_button_info['index']
        if graph_id_to_maximize in stored_figures:
            figure_to_display = stored_figures[graph_id_to_maximize]
            fullscreen_config = {'displayModeBar': True, 'responsive': True, 'useResizeHandler': True}
            title_map = {
                'daily-performance-chart': 'Daily Performance for Selected Week', # NEW
                'weekly-funnel-chart': 'Weekly Repair Center Performance',
                'month-wise-sr-tat-chart': 'Month wise SR TAT', 'tat-chart': 'Stage wise SR TAT',
                'product-family-chart': 'Product Family Analysis', 'branch-srs-chart': 'SRs per Branch',
                'status-distribution-chart': 'Simplified SR Status Distribution',
                'srs-over-time-chart': 'SRs Over Time',
                'component-failure-chart': 'Component Failure Frequency', 'ageing-status-chart': 'SR Status by Ageing Category'
            }
            modal_title = title_map.get(graph_id_to_maximize, "Full Screen View")
            return True, figure_to_display, fullscreen_config, modal_title
        else:
            print(f"Warning: Figure for '{graph_id_to_maximize}' not found in stored_figures.")
            return True, get_empty_figure_with_message("Figure not available for full screen."), {'displayModeBar': True, 'responsive': True, 'useResizeHandler': True}, "Error"
    if is_modal_open:
        if ctx.triggered_id == 'fullscreen-modal' and ctx.triggered_prop_ids == {'fullscreen-modal.is_open': False}:
            return False, dash.no_update, dash.no_update, dash.no_update
    raise dash.exceptions.PreventUpdate

@app.callback(
    Output('daily-performance-chart', 'figure'), # NEW
    Output('weekly-funnel-chart', 'figure'),
    Output('branch-srs-chart', 'figure'),
    Output('status-distribution-chart', 'figure'),
    Output('srs-over-time-chart', 'figure'),
    Output('component-failure-chart', 'figure'),
    Output('ageing-status-chart', 'figure'),
    Output('product-family-chart', 'figure'),
    Output('tat-chart', 'figure'),
    Output('month-wise-sr-tat-chart', 'figure'),
    Output('stored-graph-figures', 'data'),

    Input('stored-data', 'data'),
    Input('problem-type-filter', 'value'),
    Input('status-filter', 'value'),
    Input('ageing-category-filter', 'value'),
    Input('product-family-filter', 'value'),
    Input('ageing-category-chart-filter', 'value'),
    Input('global-date-picker-range', 'start_date'),
    Input('global-date-picker-range', 'end_date'),
    Input('week-selector-dropdown', 'value'), # NEW
    Input('initial-animation-trigger', 'n_intervals')
)
@cache.memoize(timeout=60)
def update_all_charts(data_json, selected_problem_type, selected_status,
                      global_selected_ageing_category, selected_product_family_group_input,
                      chart_specific_ageing_category,
                      global_start_date, global_end_date,
                      selected_week, # NEW
                      initial_animation_n_intervals):

    actual_problem_type = get_filter_value(selected_problem_type)
    actual_status = get_filter_value(selected_status)
    actual_global_ageing_category = get_filter_value(global_selected_ageing_category)
    actual_selected_product_family_group = get_filter_value(selected_product_family_group_input)
    actual_chart_specific_ageing_category = get_filter_value(chart_specific_ageing_category)
    actual_selected_week = get_filter_value(selected_week)

    df = read_df_from_json(data_json)
    figures = {}

    if df.empty:
        msg = "Filtered data is empty. Adjust filters in the desktop app or dashboard."
        fig_daily = get_empty_figure_with_message(msg)
        fig_funnel = get_empty_figure_with_message(msg)
        fig_branch = get_empty_figure_with_message(msg)
        fig_status = get_empty_figure_with_message(msg)
        fig_time = get_empty_figure_with_message(msg)
        fig_component = get_empty_figure_with_message(msg)
        fig_product_family = get_empty_figure_with_message(msg)
        fig_tat = get_empty_figure_with_message(msg)
        fig_month_wise_sr_tat = get_empty_figure_with_message(msg)
        fig_ageing_status = get_empty_figure_with_message(msg)
    else:
        df_base_filtered_by_dates = df.copy()

        if global_start_date and 'SR creation date' in df_base_filtered_by_dates.columns:
            start_date_obj = pd.to_datetime(global_start_date).normalize()
            df_base_filtered_by_dates = df_base_filtered_by_dates[df_base_filtered_by_dates['SR creation date'] >= start_date_obj].copy()

        if global_end_date and 'SR creation date' in df_base_filtered_by_dates.columns:
            end_date_obj = pd.to_datetime(global_end_date).normalize()
            df_base_filtered_by_dates = df_base_filtered_by_dates[df_base_filtered_by_dates['SR creation date'] <= end_date_obj + timedelta(days=1, microseconds=-1)].copy()

        # The new daily chart uses a different filtering mechanism, so we start with the full dataset
        df_for_daily_chart = df.copy()
        df_globally_filtered_for_charts = df_base_filtered_by_dates.copy()

        # Apply global filters to both dataframes
        if actual_problem_type != 'All':
            df_for_daily_chart = df_for_daily_chart[df_for_daily_chart['SR Problem type'].isin(actual_problem_type if isinstance(actual_problem_type, list) else [actual_problem_type])].copy()
            df_globally_filtered_for_charts = df_globally_filtered_for_charts[df_globally_filtered_for_charts['SR Problem type'].isin(actual_problem_type if isinstance(actual_problem_type, list) else [actual_problem_type])].copy()
        if actual_status != 'All':
            df_for_daily_chart = df_for_daily_chart[df_for_daily_chart['Simplified SR Status'].isin(actual_status if isinstance(actual_status, list) else [actual_status])].copy()
            df_globally_filtered_for_charts = df_globally_filtered_for_charts[df_globally_filtered_for_charts['Simplified SR Status'].isin(actual_status if isinstance(actual_status, list) else [actual_status])].copy()
        if actual_global_ageing_category != 'All':
            df_for_daily_chart = df_for_daily_chart[df_for_daily_chart['Ageing Category'].isin(actual_global_ageing_category if isinstance(actual_global_ageing_category, list) else [actual_global_ageing_category])].copy()
            df_globally_filtered_for_charts = df_globally_filtered_for_charts[df_globally_filtered_for_charts['Ageing Category'].isin(actual_global_ageing_category if isinstance(actual_global_ageing_category, list) else [actual_global_ageing_category])].copy()
        if actual_selected_product_family_group != 'All':
            df_for_daily_chart = df_for_daily_chart[df_for_daily_chart['Product Family Group'].isin(actual_selected_product_family_group if isinstance(actual_selected_product_family_group, list) else [actual_selected_product_family_group])].copy()
            df_globally_filtered_for_charts = df_globally_filtered_for_charts[df_globally_filtered_for_charts['Product Family Group'].isin(actual_selected_product_family_group if isinstance(actual_selected_product_family_group, list) else [actual_selected_product_family_group])].copy()


        if df_globally_filtered_for_charts.empty:
            msg_general = "No data after applying global filters."
            fig_funnel = get_empty_figure_with_message(msg_general)
            fig_branch = get_empty_figure_with_message(msg_general)
            fig_status = get_empty_figure_with_message(msg_general)
            fig_time = get_empty_figure_with_message(msg_general)
            fig_component = get_empty_figure_with_message(msg_general)
            fig_product_family = get_empty_figure_with_message(msg_general)
            fig_tat = get_empty_figure_with_message(msg_general)
            fig_month_wise_sr_tat = get_empty_figure_with_message(msg_general)
            fig_ageing_status = get_empty_figure_with_message(msg_general)
        else:
            df_unique_srs_globally_filtered_reduced = df_globally_filtered_for_charts.drop_duplicates(subset=['SR no.']).copy()
            df_full_for_categorization = df_globally_filtered_for_charts.copy()

            fig_funnel = create_simplified_weekly_chart(df_full_for_categorization)
            fig_branch = create_srs_per_branch_chart(df_unique_srs_globally_filtered_reduced)
            fig_status = create_sr_status_distribution_chart(df_unique_srs_globally_filtered_reduced)
            fig_time = create_srs_over_time_chart(df_unique_srs_globally_filtered_reduced)
            fig_component = create_component_failure_frequency_chart(df_full_for_categorization)
            fig_product_family = create_product_family_analysis_chart(df_unique_srs_globally_filtered_reduced, actual_problem_type, actual_status, actual_global_ageing_category, actual_selected_product_family_group)

            fig_tat = create_stage_wise_sr_tat_chart(df_full_for_categorization, actual_problem_type, actual_selected_product_family_group)
            fig_month_wise_sr_tat = create_month_wise_sr_tat_chart(df_full_for_categorization)
            fig_ageing_status = create_sr_status_by_ageing_category_chart(df_full_for_categorization, actual_chart_specific_ageing_category)
        
        # Always generate the daily chart, even if other charts are empty due to date range picker.
        fig_daily = create_daily_performance_chart(df_for_daily_chart, actual_selected_week)


    figures['daily-performance-chart'] = fig_daily
    figures['weekly-funnel-chart'] = fig_funnel
    figures['branch-srs-chart'] = fig_branch
    figures['status-distribution-chart'] = fig_status
    figures['srs-over-time-chart'] = fig_time
    figures['component-failure-chart'] = fig_component
    figures['ageing-status-chart'] = fig_ageing_status
    figures['product-family-chart'] = fig_product_family
    figures['tat-chart'] = fig_tat
    figures['month-wise-sr-tat-chart'] = fig_month_wise_sr_tat

    return fig_daily, fig_funnel, fig_branch, fig_status, fig_time, fig_component, fig_ageing_status, fig_product_family, fig_tat, fig_month_wise_sr_tat, figures


@app.callback(
    Output("download-pdf-data", "data"),
    Input("download-pdf-button", "n_clicks"),
    State("stored-graph-figures", "data"),
    prevent_initial_call=True
)
def download_pdf(n_clicks, stored_figures_data):
    if n_clicks is None or n_clicks == 0:
        raise dash.exceptions.PreventUpdate

    if not stored_figures_data:
        print("No stored figures available for PDF generation.")
        return None
    
    # Using a larger custom page size for better layout
    CUSTOM_PAGE_SIZE = (18 * inch, 10 * inch)
    PDF_BACKGROUND_COLOR = (1, 1, 1)
    PDF_TEXT_COLOR = (0, 0, 0)

    doc_width, doc_height = CUSTOM_PAGE_SIZE
    margin_x = 0.5 * inch
    margin_y = 0.5 * inch

    CHART_EXPORT_WIDTH_PX = 1200 # Increased width for clarity
    CHART_EXPORT_HEIGHT_PX = 600 # Increased height for clarity
    RENDER_SCALE = 1.0

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=CUSTOM_PAGE_SIZE)

    def apply_page_background(canvas_obj):
        canvas_obj.setFillColorRGB(*PDF_BACKGROUND_COLOR)
        canvas_obj.rect(0, 0, doc_width, doc_height, fill=1)

    apply_page_background(c)
    current_y = doc_height - margin_y

    c.setFillColorRGB(*PDF_TEXT_COLOR)
    c.setFont("Helvetica-Bold", 24)
    c.drawCentredString(doc_width / 2.0, current_y - 0.5 * inch, "SECURE Dashboard Report")
    current_y -= 0.5 * inch

    c.setFillColorRGB(*PDF_TEXT_COLOR)
    c.setFont("Helvetica", 12)
    c.drawString(margin_x, current_y - 0.25 * inch, f"Generated On: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    current_y -= 0.5 * inch

    chart_order = [
        'daily-performance-chart', 'weekly-funnel-chart', 'month-wise-sr-tat-chart', 'ageing-status-chart', 'tat-chart',
        'product-family-chart', 'branch-srs-chart', 'status-distribution-chart',
        'srs-over-time-chart', 'component-failure-chart'
    ]
    chart_titles = {
        'daily-performance-chart': 'Daily Performance for Selected Week',
        'weekly-funnel-chart': 'Weekly Repair Center Performance',
        'month-wise-sr-tat-chart': 'Month wise SR TAT', 'tat-chart': 'Stage wise SR TAT',
        'product-family-chart': 'Product Family Analysis', 'branch-srs-chart': 'SRs per Branch',
        'status-distribution-chart': 'Simplified SR Status Distribution',
        'srs-over-time-chart': 'SRs Over Time',
        'component-failure-chart': 'Component Failure Frequency',
        'ageing-status-chart': 'SR Status by Ageing Category'
    }

    print("Assembling PDF from live rendered images.")
    for i, chart_id in enumerate(chart_order):
        if chart_id not in stored_figures_data:
            print(f"Figure data not available for '{chart_id}'. Skipping.")
            continue

        fig_dict = stored_figures_data[chart_id]
        fig = go.Figure(fig_dict)

        fig.update_layout(paper_bgcolor='white', plot_bgcolor='white', font=dict(color='black'))

        img_buffer_chart = BytesIO()
        try:
            img_bytes = fig.to_image(format="png", width=CHART_EXPORT_WIDTH_PX, height=CHART_EXPORT_HEIGHT_PX, scale=RENDER_SCALE)
            img_buffer_chart.write(img_bytes)
            img_buffer_chart.seek(0)
            img_reportlab = ImageReader(img_buffer_chart)

            img_pixel_width, img_pixel_height = img_reportlab.getSize()

            if i > 0:
                c.showPage()
                apply_page_background(c)
                current_y = doc_height - margin_y

            available_width_pdf = doc_width - (2 * margin_x)
            available_height_pdf = current_y - margin_y - (1 * inch) # leave space for title

            # Scale image to fit within the available space while maintaining aspect ratio
            scale_factor = min(available_width_pdf / (img_pixel_width / 72.0), available_height_pdf / (img_pixel_height / 72.0))
            img_width_on_pdf = (img_pixel_width / 72.0) * scale_factor
            img_height_on_pdf = (img_pixel_height / 72.0) * scale_factor


            title_text = chart_titles.get(chart_id, "Chart Title Missing")
            title_font_size = 18

            styles = getSampleStyleSheet()
            title_style = styles['Normal']
            title_style.alignment = 1
            title_style.fontName = "Helvetica-Bold"
            title_style.fontSize = title_font_size
            title_style.textColor = PDF_TEXT_COLOR
            title_paragraph = Paragraph(title_text, title_style)

            title_frame_width = available_width_pdf
            title_actual_width, title_actual_height = title_paragraph.wrapOn(c, title_frame_width, doc_height)

            # Center the whole block (title + image)
            total_block_height = title_actual_height + (0.2 * inch) + img_height_on_pdf
            y_start_pos = current_y - (current_y - margin_y - total_block_height) / 2

            title_paragraph.drawOn(c, margin_x, y_start_pos - title_actual_height)

            image_x_pos = margin_x + (available_width_pdf - img_width_on_pdf) / 2.0
            image_y_pos = y_start_pos - title_actual_height - (0.2 * inch) - img_height_on_pdf

            c.drawImage(img_reportlab, image_x_pos, image_y_pos, width=img_width_on_pdf, height=img_height_on_pdf)

        except Exception as e:
            print(f"Error exporting or adding chart '{chart_id}' to PDF: {e}")
            if i > 0:
                c.showPage()
                apply_page_background(c)
                current_y = doc_height - margin_y

            c.setFillColorRGB(*PDF_TEXT_COLOR)
            c.setFont("Helvetica-Bold", 14)
            c.drawCentredString(doc_width / 2.0, current_y - 1 * inch, f"Error: Could not render chart '{chart_titles.get(chart_id, chart_id)}'")
            c.setFont("Helvetica", 10)
            c.drawCentredString(doc_width / 2.0, current_y - 1.5 * inch, f"Details: {e}")

    c.save()
    buffer.seek(0)
    print("PDF assembled successfully from live rendered images.")
    return dcc.send_bytes(buffer.getvalue(), f"Dash_Dashboard_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")


def run_dash_app_instance(shared_data_proxy_arg, click_queue_arg):
    global app, shared_data_proxy_global, click_data_queue_global
    shared_data_proxy_global = shared_data_proxy_arg
    click_data_queue_global = click_queue_arg
    app.run(debug=False, host='127.0.0.1', port=8050)

def start_dash_server_threadsafe(shared_data_proxy_arg, click_queue_arg):
    run_dash_app_instance(shared_data_proxy_arg, click_queue_arg)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    local_manager = multiprocessing.Manager()
    local_shared_data_proxy = local_manager.dict()
    local_click_queue = local_manager.Queue()

    dummy_df = get_empty_dataframe_structure()
    local_shared_data_proxy['live_data'] = process_df_to_json(dummy_df)

    start_dash_server_threadsafe(local_shared_data_proxy, local_click_queue)