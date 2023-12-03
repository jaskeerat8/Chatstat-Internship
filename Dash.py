# Importing Libraries
import dash
from dash import dcc
from dash import html
import plotly.express as px
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
from dash_iconify import DashIconify
from dash.dependencies import Input, Output
from datetime import datetime, date, timedelta
import io, json, boto3
import pandas as pd
import mysql.connector
import warnings

# Reading Data
df = pd.read_csv("data/final_data.csv")
platform_colors = {"Instagram": "#25D366", "Twitter": "#2D96FF", "Facebook": "#FF5100", "Tiktok": "#f6c604"}
alert_colors = {"High": "#FF5100", "Medium": "#FFD334", "Low": "#25D366"}

# User Table
alert_table_df = df[(df["alert_contents"].str.lower() != "no") & (df["alert_contents"].str.lower() != "")]
alert_table_df = alert_table_df.groupby(by=["user_childrens", "name_childrens"], as_index=False)["id_contents"].nunique()
alert_table_df = alert_table_df.sort_values(by=["id_contents"], ascending=False)
alert_table_df = alert_table_df.reset_index(drop=True)
alert_table_df.columns = ["user", "name", "count"]
user_list = alert_table_df["name"].unique()

# SideBar
sidebar = html.Div(
    children=[
        html.Div(
            [
                html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/chatstatlogo.png"),
                html.H2("chatstat")
            ],
            className="sidebar-header"
        ),
        html.Hr(style={"height": "8px", "width": "100%", "backgroundColor": "#25d366", "opacity": "1", "borderRadius": "5px", "margin-top": "0px",
                       "margin-left": "0px", "margin-right": "0px"}),
        dbc.Nav(
            children=[
                dbc.NavLink([html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/dashboard.png"), html.Span("Dashboard")],
                            href="/dashboard", active="exact"),
                dbc.NavLink([html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/report.png"), html.Span("Report")],
                            href="/report", active="exact"),
                dbc.NavLink([html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/analytics.png"), html.Span("Analytics")],
                            href="/analytics", active="exact"),
            ],
            vertical=True,
            pills=True,
            className="sidebar-navlink"
        ),
    ],
    className="sidebar",
)

# Header
header = dmc.Header(height="60px", fixed=False, children=[
            dmc.Text("Dashboard Analytics", color="#25D366",
                     style={"fontFamily": "Poppins, sans-serif", "fontWeight": "bold", "fontSize": 25}),
            html.Div([
                dcc.Dropdown(id="child_control", value=user_list[0],
                             options=[{"value": i, "label": html.Div([
                                 dmc.Avatar(''.join([word[0] for word in i.split(" ")]), color="red", size="md", radius="xl", style={"margin-right": "8px"}),
                                 html.P(i.title(), style={"margin": "0"})
                             ], style={"display": "flex", "align-items": "center"})} for i in user_list],
                             placeholder="Select User", clearable=False, searchable=False,
                             style={"width": "auto", "minWidth": "120px", "margin-left": "auto", "border": "none", "boxShadow": "none"}
                             ),
                ], style={"display": "flex", "align-items": "center", "justifyContent": "flex-end"}
            ),
        ],
    style={"display": "flex", "align-items": "center", "justifyContent": "space-between", "padding": "0px 20px 0px 0px"},
    className="header"
)

# Controls
control = html.Div([
    html.Div([
        dmc.SegmentedControl(
            id="time_control",
            value="1", radius="md", size="xs",
            data=[
                {"label": "Daily", "value": "1"},
                {"label": "Weekly", "value": "7"},
                {"label": "Monthly", "value": "30"},
                {"label": "Yearly", "value": "365"},
                {"label": "Custom Range", "value": "custom"}
            ]
        ),
        html.Div(id="custom_date_range", children=[
            dmc.DateRangePicker(id="date_picker",
                value=[datetime.now().date()-timedelta(days=60), datetime.now().date()],
                style={"width": 300},
            )
        ], style={"display": "none"}),
    ], style={"display": "flex", "flex-direction": "row"}),
    dcc.Dropdown(id="platform_dropdown", value=user_list[0],
        options=[{"value": i, "label": i.title()} for i in df["platform_contents"].unique() if str(i) != "nan"] + [{"value": "all", "label": "All"}],
        placeholder="Social Platform", clearable=False, searchable=False,
        className="platform_dropdown", style={"width": 200, "borderRadius": 10, "text-align": "center"}
    ),
    dmc.TextInput(id="searchbar",placeholder="Search...", icon=DashIconify(icon="healthicons:magnifying-glass"), style={"width": 300, "borderRadius": 100}),
], style={"display": "flex", "justify-content": "space-between"}
)

# Designing Main App
app = dash.Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=[dbc.themes.MATERIA, dbc.icons.FONT_AWESOME, 'https://fonts.googleapis.com/css2?family=Poppins:wght@500&display=swap'])
app.layout = html.Div(
    children=[
        dcc.Location(id='url-path', refresh=False),
        html.Div(children=[], style={'width': '5rem', 'display': 'inline-block'}),
        html.Div(id='page-content', style={'display': 'inline-block', 'width': 'calc(100% - 5rem)'})
    ]
)


# Website Page Navigation
@app.callback(Output("page-content", "children"),
              [Input("url-path", "pathname")])
def display_page(pathname):
    if pathname == "/dashboard":
        page = [sidebar, header, control,
            html.Div(children=[dcc.Graph(id="content_risk_bar_chart", style={"width": "100%", "margin": "5px 2.5px 5px 5px"}),
                               dcc.Graph(id="comment_alert_line_chart", style={"width": "100%", "margin": "5px 2.5px 5px 5px"})],
                     style={"display": "flex", "width": "100%", "margin": 0, "padding": 0})
        ]
        return page
    elif pathname == "/report":
        return [sidebar]
    elif pathname == "/analytics":
        return [sidebar]
    else:
        return [sidebar]


# Custom Date Picker
@app.callback(
    Output("custom_date_range", "style"),
    [Input("time_control", "value")]
)
def show_custom_date_filter(value):
    if(value == "custom"):
        return {"display": "block", "margin-left": "10px"}
    else:
        return {"display": "none", "margin-left": "10px"}


# Content Risk Bar Chart
@app.callback(
    Output("content_risk_bar_chart", "figure"),
    [Input("platform_dropdown", "value")]
)
def update_bar_chart(platform_value):
    risk_contents_df = df.copy()

    if((platform_value is not None) and (platform_value != "all")):
        risk_contents_df = risk_contents_df[risk_contents_df["platform_contents"] == platform_value]

    risk_contents_df["createTime_contents"] = pd.to_datetime(risk_contents_df["createTime_contents"], format="%Y-%m-%d %H:%M:%S.%f")
    risk_contents_df = risk_contents_df[(risk_contents_df["alert_contents"].str.lower() != "no") & (risk_contents_df["alert_contents"].str.lower() != "")]
    risk_contents_df = risk_contents_df.groupby(by=["alert_contents", "platform_contents"], as_index=False)["id_contents"].nunique()
    risk_contents_df.columns = ["alert", "platform", "count"]
    risk_contents_df["alert"] = pd.Categorical(risk_contents_df["alert"], categories=["High", "Medium", "Low"], ordered=True)
    risk_contents_df = risk_contents_df.sort_values(by="alert")

    if((platform_value is None) or (platform_value == "all")):
        content_risk = px.bar(risk_contents_df, x="alert", y="count", color="platform", text_auto=True, color_discrete_map=platform_colors, pattern_shape_sequence=None)
    else:
        content_risk = px.bar(risk_contents_df, x="alert", y="count", color="alert", text_auto=True, color_discrete_map=alert_colors, pattern_shape_sequence=None)
    content_risk.update_layout(legend=dict(traceorder="grouped", orientation="h", x=1, y=1, xanchor="right", yanchor="bottom", title_text=""))
    content_risk.update_traces(width=0.4, marker_line=dict(color='black', width=1))
    content_risk.update_layout(xaxis_title="", yaxis_title="", legend_title_text="", plot_bgcolor="rgba(0, 0, 0, 0)", title="<b>Alerts on User Content</b>")
    content_risk.update_layout(yaxis_showgrid=True, yaxis = dict(tickfont=dict(size=12, family="Poppins, sans-serif", color="#8E8E8E"), griddash = "dash", gridwidth = 1, gridcolor = "#DADADA"))
    content_risk.update_layout(xaxis_showgrid=False, xaxis=dict(tickfont=dict(size=18, family="Poppins, sans-serif", color="#052F5F")))
    content_risk.update_xaxes(fixedrange=True)
    content_risk.update_yaxes(fixedrange=True)
    return content_risk


# Comment Alert Line Chart
@app.callback(
    Output("comment_alert_line_chart", "figure"),
    [Input("platform_dropdown", "value")]
)
def update_line_chart(platform_value):
    alert_comments_df = df.copy()

    alert_comments_df["createTime_contents"] = pd.to_datetime(alert_comments_df["createTime_contents"], format="%Y-%m-%d %H:%M:%S.%f")
    alert_comments_df = alert_comments_df[(alert_comments_df["alert_comments"].str.lower() != "no") & (alert_comments_df["alert_comments"].str.lower() != "")]
    alert_comments_df["commentTime_comments"] = pd.to_datetime(alert_comments_df["commentTime_comments"]).dt.strftime("%b %Y")
    alert_comments_df = alert_comments_df.groupby(by=["commentTime_comments", "platform_comments"], as_index=False)["id_contents"].nunique()
    alert_comments_df.columns = ["commentTime", "platform", "count"]
    alert_comments_df["commentTime"] = pd.to_datetime(alert_comments_df["commentTime"], format="%b %Y")
    alert_comments_df.sort_values(by="commentTime", inplace=True)

    comment_alert = px.line(alert_comments_df, x="commentTime", y="count", color="platform", color_discrete_map=platform_colors)
    comment_alert.update_layout(legend=dict(traceorder="grouped", orientation="h", x=1, y=1, xanchor="right", yanchor="bottom", title_text=""))
    comment_alert.update_layout(xaxis_title="", yaxis_title="", legend_title_text="", plot_bgcolor="rgba(0, 0, 0, 0)", title="<b>Alerts on Comments Received</b>")
    comment_alert.update_layout(yaxis_showgrid=True, yaxis = dict(tickfont=dict(size=12, family="Poppins, sans-serif", color="#8E8E8E"), griddash = "dash", gridwidth = 1, gridcolor = "#DADADA"))
    comment_alert.update_layout(xaxis_showgrid=False, xaxis=dict(tickfont=dict(size=18, family="Poppins, sans-serif", color="#052F5F")))
    comment_alert.update_traces(mode="lines+markers", line=dict(width=2), marker=dict(sizemode="diameter", size=8, color="white", line=dict(width=2)))
    comment_alert.update_xaxes(fixedrange=True)
    comment_alert.update_yaxes(fixedrange=True)
    return comment_alert


# Running Main App
if __name__ == "__main__":
    app.run_server(debug=True)
