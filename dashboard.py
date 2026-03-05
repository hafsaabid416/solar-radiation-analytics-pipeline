

import os
import logging
import warnings

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from dash import Dash, dcc, html, Input, Output, callback_context
import dash_bootstrap_components as dbc

warnings.filterwarnings("ignore")

log = logging.getLogger("dashboard")
logging.basicConfig(level=logging.INFO, format="%(asctime)s  [%(levelname)s]  %(message)s")

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///bradford_weather.db")
CSV_PATH     = os.getenv("CSV_PATH", "DATA2.csv")




def bootstrap_database():
    """Run the ETL pipeline if the database doesn't exist yet."""
    from etl_pipeline import db_exists, run_pipeline
    if not db_exists(DATABASE_URL):
        log.info("Database not found — running ETL pipeline now …")
        run_pipeline(csv_path=CSV_PATH, database_url=DATABASE_URL)
    else:
        log.info("Database found — skipping ETL pipeline.")


bootstrap_database()




from etl_pipeline import load_table

log.info("Loading analytics tables from database …")

daily_stats = load_table("daily_stats", DATABASE_URL)
hourly_avg  = load_table("hourly_avg",  DATABASE_URL)
monthly     = load_table("monthly_stats", DATABASE_URL)

# Parse date columns
daily_stats["date"] = pd.to_datetime(daily_stats["date"])
daily_stats["Date"] = daily_stats["date"].dt.date


daily_stats = daily_stats.sort_values("date").reset_index(drop=True)
daily_stats["cumulative_energy"] = daily_stats["solar_energy_sum"].cumsum()

log.info(f"Daily rows   : {len(daily_stats):,}")
log.info(f"Hourly rows  : {len(hourly_avg)}")
log.info(f"Monthly rows : {len(monthly)}")
log.info(f"Date range   : {daily_stats['date'].min().date()} → {daily_stats['date'].max().date()}")

# Derived summary stats for header cards
total_obs       = len(daily_stats) * 48      # approx (30-min intervals)
max_solar_rad   = daily_stats["solar_rad_max"].max()
mean_solar_rad  = daily_stats["solar_rad_mean"].mean()
total_energy    = daily_stats["solar_energy_sum"].sum()
mean_uv         = daily_stats["uv_index_mean"].mean()
max_uv          = daily_stats["uv_index_max"].max()
high_risk_days  = int((daily_stats["uv_index_max"] >= 6).sum())
date_min        = daily_stats["date"].min()
date_max        = daily_stats["date"].max()




app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Bradford Weather Station Dashboard"

app.layout = dbc.Container([

    
    dbc.Row([
        dbc.Col([
            html.H1("☀️ Solar Energy & UV Analytics Dashboard",
                    className="text-center text-primary mb-2"),
            html.H5("Bradford Weather Station — Interactive Data Explorer",
                    className="text-center text-secondary mb-1"),
            html.P("Data sourced via automated ETL pipeline from OTT Parsivel² / Vantage Pro2 sensors",
                   className="text-center text-muted small mb-3"),
            html.Hr()
        ])
    ]),

    
    dbc.Row([
        dbc.Col([
            dbc.Card([dbc.CardBody([
                html.H4("📊 Dataset", className="card-title"),
                html.P(f"~{total_obs:,} total observations", className="mb-1"),
                html.P(f"{date_min.strftime('%d/%m/%Y')} – {date_max.strftime('%d/%m/%Y')}", className="mb-1"),
                html.P(f"{len(daily_stats):,} days recorded", className="mb-0"),
                html.Small("ETL pipeline: CSV → DB", className="text-muted")
            ])], color="light")
        ], width=4),

        dbc.Col([
            dbc.Card([dbc.CardBody([
                html.H4("☀️ Solar Radiation", className="card-title"),
                html.P(f"Mean: {mean_solar_rad:.2f} W/m²", className="mb-1"),
                html.P(f"Peak: {max_solar_rad:.2f} W/m²", className="mb-1"),
                html.P(f"Total energy: {total_energy:.0f} Ly", className="mb-0")
            ])], color="warning", inverse=True)
        ], width=4),

        dbc.Col([
            dbc.Card([dbc.CardBody([
                html.H4("🔆 UV Index", className="card-title"),
                html.P(f"Mean: {mean_uv:.2f}", className="mb-1"),
                html.P(f"Maximum: {max_uv:.2f}", className="mb-1"),
                html.P(f"High-risk days (UV≥6): {high_risk_days}", className="mb-0")
            ])], color="danger", inverse=True)
        ], width=4),
    ], className="mb-4"),

    
    dbc.Row([
        dbc.Col([
            dbc.Card([dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        html.H5("📅 Date Range", className="mb-2"),
                        dcc.DatePickerRange(
                            id="date-picker",
                            min_date_allowed=date_min,
                            max_date_allowed=date_max,
                            initial_visible_month=date_min,
                            start_date=date_min,
                            end_date=date_max,
                            display_format="DD/MM/YYYY"
                        )
                    ], width=6),
                    dbc.Col([
                        html.H5("🗓 Quick Select", className="mb-2"),
                        dbc.ButtonGroup([
                            dbc.Button("Full Year", id="btn-full",   size="sm", color="primary",   className="me-1"),
                            dbc.Button("Summer",    id="btn-summer", size="sm", color="warning",   className="me-1"),
                            dbc.Button("Winter",    id="btn-winter", size="sm", color="info",      className="me-1"),
                            dbc.Button("Spring",    id="btn-spring", size="sm", color="success",   className="me-1"),
                            dbc.Button("Autumn",    id="btn-autumn", size="sm", color="secondary"),
                        ])
                    ], width=6)
                ])
            ])]
        )
    ], className="mb-4"),

   
    dbc.Row([
        dbc.Col([
            dbc.Card([dbc.CardBody([
                html.H5("📈 Daily Solar Radiation & UV Index Trends"),
                dcc.Graph(id="timeseries-plot")
            ])])
        ])
    ], className="mb-4"),

    
    dbc.Row([
        dbc.Col([
            dbc.Card([dbc.CardBody([
                html.H5("🕐 Average Diurnal Pattern"),
                dcc.Graph(id="diurnal-plot")
            ])])
        ], width=6),
        dbc.Col([
            dbc.Card([dbc.CardBody([
                html.H5("📊 Seasonal Distribution"),
                dcc.Graph(id="seasonal-plot")
            ])])
        ], width=6),
    ], className="mb-4"),

    
    dbc.Row([
        dbc.Col([
            dbc.Card([dbc.CardBody([
                html.H5("🔥 Solar Radiation Heatmap (Hour × Date)"),
                dcc.Graph(id="heatmap-plot")
            ])])
        ], width=6),
        dbc.Col([
            dbc.Card([dbc.CardBody([
                html.H5("🔗 Solar Radiation vs Temperature"),
                dcc.Graph(id="correlation-plot")
            ])])
        ], width=6),
    ], className="mb-4"),

    
    dbc.Row([
        dbc.Col([
            dbc.Card([dbc.CardBody([
                html.H5("⚡ Cumulative Energy Harvest"),
                dcc.Graph(id="cumulative-plot")
            ])])
        ], width=6),
        dbc.Col([
            dbc.Card([dbc.CardBody([
                html.H5("⚠️ UV Risk Distribution"),
                dcc.Graph(id="uv-risk-plot")
            ])])
        ], width=6),
    ], className="mb-4"),

    
    dbc.Row([
        dbc.Col([
            dbc.Card([dbc.CardBody([
                html.H5("📅 Monthly Solar Energy & UV Overview"),
                dcc.Graph(id="monthly-plot")
            ])])
        ])
    ], className="mb-4"),

    
    dbc.Row([
        dbc.Col([
            html.Hr(),
            html.P(
                f"Bradford Weather Station Dashboard  |  COS7046-B Big Data Visualization  |  "
                f"Database: {DATABASE_URL.split('://')[0].upper()}",
                className="text-center text-muted small"
            )
        ])
    ])

], fluid=True, style={"backgroundColor": "#f8f9fa"})




@app.callback(
    [Output("date-picker", "start_date"),
     Output("date-picker", "end_date")],
    [Input("btn-full",   "n_clicks"),
     Input("btn-summer", "n_clicks"),
     Input("btn-winter", "n_clicks"),
     Input("btn-spring", "n_clicks"),
     Input("btn-autumn", "n_clicks")],
    prevent_initial_call=True
)
def quick_select(btn_full, btn_summer, btn_winter, btn_spring, btn_autumn):
    ctx = callback_context
    if not ctx.triggered:
        return date_min, date_max

    btn_id = ctx.triggered[0]["prop_id"].split(".")[0]

    season_rows = daily_stats[daily_stats["season"] == {
        "btn-summer": "Summer",
        "btn-winter": "Winter",
        "btn-spring": "Spring",
        "btn-autumn": "Autumn",
    }.get(btn_id, "")]

    if btn_id == "btn-full" or season_rows.empty:
        return date_min, date_max

    return season_rows["date"].min(), season_rows["date"].max()




@app.callback(
    [Output("timeseries-plot",  "figure"),
     Output("diurnal-plot",     "figure"),
     Output("seasonal-plot",    "figure"),
     Output("heatmap-plot",     "figure"),
     Output("correlation-plot", "figure"),
     Output("cumulative-plot",  "figure"),
     Output("uv-risk-plot",     "figure"),
     Output("monthly-plot",     "figure")],
    [Input("date-picker", "start_date"),
     Input("date-picker", "end_date")]
)
def update_plots(start_date, end_date):
    start = pd.to_datetime(start_date)
    end   = pd.to_datetime(end_date)

    # Filter daily stats
    mask_d = (daily_stats["date"] >= start) & (daily_stats["date"] <= end)
    fd = daily_stats[mask_d].copy()

    if fd.empty:
        empty = go.Figure()
        empty.update_layout(title="No data in selected range")
        return [empty] * 8

    
    fig_ts = make_subplots(rows=2, cols=1,
                           subplot_titles=("Solar Radiation (W/m²)", "UV Index"),
                           vertical_spacing=0.12)

    fig_ts.add_trace(go.Scatter(
        x=fd["date"], y=fd["solar_rad_mean"],
        fill="tozeroy", name="Solar Rad",
        line=dict(color="orange", width=2)
    ), row=1, col=1)

    # Max solar overlay
    fig_ts.add_trace(go.Scatter(
        x=fd["date"], y=fd["solar_rad_max"],
        name="Solar Rad Max", mode="lines",
        line=dict(color="darkorange", width=1, dash="dot"),
        opacity=0.5
    ), row=1, col=1)

    fig_ts.add_trace(go.Scatter(
        x=fd["date"], y=fd["uv_index_mean"],
        fill="tozeroy", name="UV Index",
        line=dict(color="purple", width=2)
    ), row=2, col=1)

    # WHO UV bands
    for y0, y1, col in [(0, 2, "green"), (3, 5, "yellow"), (6, 7, "orange"), (8, 10, "red")]:
        fig_ts.add_hrect(y0=y0, y1=y1, fillcolor=col, opacity=0.08,
                         layer="below", line_width=0, row=2, col=1)

    fig_ts.update_yaxes(title_text="W/m²", row=1, col=1)
    fig_ts.update_yaxes(title_text="UV Index", row=2, col=1)
    fig_ts.update_xaxes(title_text="Date", row=2, col=1)
    fig_ts.update_layout(height=600, showlegend=True,
                         legend=dict(orientation="h", y=1.05))

    
    fig_diurnal = make_subplots(specs=[[{"secondary_y": True}]])

    fig_diurnal.add_trace(go.Scatter(
        x=hourly_avg["hour"], y=hourly_avg["solar_rad_mean"],
        name="Solar Radiation", line=dict(color="orange", width=3),
        mode="lines+markers",
        error_y=dict(type="data", array=hourly_avg.get("solar_rad_std",
                     pd.Series([0]*len(hourly_avg))).fillna(0).tolist(),
                     visible=True, color="rgba(255,165,0,0.3)")
    ), secondary_y=False)

    fig_diurnal.add_trace(go.Scatter(
        x=hourly_avg["hour"], y=hourly_avg["uv_index_mean"],
        name="UV Index", line=dict(color="purple", width=3),
        mode="lines+markers"
    ), secondary_y=True)

    fig_diurnal.update_xaxes(title_text="Hour of Day")
    fig_diurnal.update_yaxes(title_text="Solar Radiation (W/m²)", secondary_y=False)
    fig_diurnal.update_yaxes(title_text="UV Index", secondary_y=True)
    fig_diurnal.update_layout(height=400, hovermode="x unified")

    
    fig_seasonal = go.Figure()
    for season, colour in [("Spring", "lightgreen"), ("Summer", "gold"),
                            ("Autumn", "coral"),      ("Winter", "lightblue")]:
        sd = fd[fd["season"] == season]
        fig_seasonal.add_trace(go.Box(
            y=sd["solar_rad_mean"], name=season,
            boxmean="sd", marker_color=colour
        ))
    fig_seasonal.update_layout(
        yaxis_title="Solar Radiation (W/m²)",
        xaxis_title="Season", height=400
    )

    
    heat_data = hourly_avg.set_index("hour")["solar_rad_mean"]
    daily_sample = fd.set_index("date")["solar_rad_mean"]

    # Matrix: rows=hours, cols=sampled dates
    sample_dates = daily_sample.index[::max(1, len(daily_sample)//52)]
    z_matrix = np.outer(
        heat_data.values / (heat_data.max() or 1),
        daily_sample[sample_dates].values
    )

    fig_heatmap = go.Figure(data=go.Heatmap(
        z=z_matrix,
        x=[d.strftime("%d/%m") for d in sample_dates],
        y=list(heat_data.index),
        colorscale="YlOrRd",
        colorbar=dict(title="W/m²")
    ))
    fig_heatmap.update_layout(
        xaxis_title="Date", yaxis_title="Hour of Day", height=400
    )

    
    scatter_df = fd[["solar_rad_mean", "temp_mean", "uv_index_mean", "season"]].dropna()
    fig_corr = px.scatter(
        scatter_df, x="solar_rad_mean", y="temp_mean",
        color="uv_index_mean", color_continuous_scale="plasma",
        symbol="season",
        labels={"solar_rad_mean": "Solar Radiation (W/m²)",
                "temp_mean":      "Temperature (°C)",
                "uv_index_mean":  "UV Index"},
        opacity=0.7
    )
    fig_corr.update_layout(height=400)

    
    fd_sorted = fd.sort_values("date")
    fd_sorted["cum_energy_filtered"] = fd_sorted["solar_energy_sum"].cumsum()

    fig_cumulative = go.Figure()
    fig_cumulative.add_trace(go.Scatter(
        x=fd_sorted["date"], y=fd_sorted["cum_energy_filtered"],
        fill="tozeroy", name="Cumulative Energy",
        line=dict(color="darkgreen", width=2)
    ))
    # Annotate final value
    fig_cumulative.add_annotation(
        x=fd_sorted["date"].iloc[-1],
        y=fd_sorted["cum_energy_filtered"].iloc[-1],
        text=f"{fd_sorted['cum_energy_filtered'].iloc[-1]:.0f} Ly",
        showarrow=True, arrowhead=2, font=dict(size=12, color="darkgreen")
    )
    fig_cumulative.update_layout(
        xaxis_title="Date", yaxis_title="Cumulative Energy (Ly)", height=400
    )

    
    uv_risk_counts = fd["uv_index_max"].apply(_uv_label).value_counts()
    colors_map = {
        "Low":       "green",
        "Moderate":  "#c8c800",
        "High":      "orange",
        "Very High": "red",
        "Extreme":   "purple"
    }
    fig_uv = go.Figure(data=[go.Pie(
        labels=uv_risk_counts.index,
        values=uv_risk_counts.values,
        marker=dict(colors=[colors_map.get(c, "gray") for c in uv_risk_counts.index]),
        hole=0.35
    )])
    fig_uv.update_layout(
        title_text="Daily Peak UV Risk Category",
        height=400
    )

    
    month_filter = monthly[
        (pd.to_datetime(monthly["year"].astype(str) + "-" +
                        monthly["month"].astype(str).str.zfill(2) + "-01") >= start) &
        (pd.to_datetime(monthly["year"].astype(str) + "-" +
                        monthly["month"].astype(str).str.zfill(2) + "-01") <= end)
    ].copy()

    month_filter["label"] = (month_filter["month_name"].str[:3] + " " +
                              month_filter["year"].astype(str))

    fig_monthly = make_subplots(specs=[[{"secondary_y": True}]])
    fig_monthly.add_trace(go.Bar(
        x=month_filter["label"], y=month_filter["solar_energy_sum"],
        name="Solar Energy (Ly)", marker_color="orange", opacity=0.8
    ), secondary_y=False)
    fig_monthly.add_trace(go.Scatter(
        x=month_filter["label"], y=month_filter["uv_index_mean"],
        name="Mean UV Index", mode="lines+markers",
        line=dict(color="purple", width=2)
    ), secondary_y=True)
    fig_monthly.update_yaxes(title_text="Solar Energy (Ly)", secondary_y=False)
    fig_monthly.update_yaxes(title_text="Mean UV Index", secondary_y=True)
    fig_monthly.update_layout(height=400, hovermode="x unified")

    return (fig_ts, fig_diurnal, fig_seasonal, fig_heatmap,
            fig_corr, fig_cumulative, fig_uv, fig_monthly)


def _uv_label(uv):
    if pd.isna(uv):  return "Unknown"
    if uv < 3:       return "Low"
    elif uv < 6:     return "Moderate"
    elif uv < 8:     return "High"
    elif uv < 11:    return "Very High"
    else:            return "Extreme"




if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  DASHBOARD READY")
    print("=" * 60)
    print(f"  DB backend : {DATABASE_URL.split('://')[0].upper()}")
    print(f"  Navigate to: http://127.0.0.1:8050/")
    print("  Press CTRL+C to stop")
    print("=" * 60 + "\n")
    app.run(debug=True, port=8050)
