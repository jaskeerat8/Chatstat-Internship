# Importing Libraries
import radial_bar_chart
import json, ast
import numpy as np
import pandas as pd
import calendar
from datetime import datetime, date, timedelta
import plotly.express as px
import plotly.graph_objects as go
import dash_mantine_components as dmc
import dash_bootstrap_components as dbc
from dash_iconify import DashIconify
from dash import Dash, html, dcc, Input, Output

# Global Variables
global date_dict
todays_date = datetime(2023, 12, 31)

# Reading static Data, later read from AWS after user authentication
df = pd.read_csv("Data/final_30-11-2023_14_11_18.csv")

# Defining Colors and Plotly Graph Options
plot_config = {"modeBarButtonsToRemove": ["zoom2d", "pan2d", "select2d", "lasso2d", "zoomIn2d", "zoomOut2d", "autoScale2d", "resetScale2d", "hoverClosestCartesian", "hoverCompareCartesian"],
               "staticPlot": False, "displaylogo": False}
platform_colors = {"Instagram": "#25D366", "Twitter": "#2D96FF", "Facebook": "#FF5100", "Tiktok": "#f6c604"}
alert_colors = {"High": "#FF5100", "Medium": "#f6c604", "Low": "#25D366"}
comment_classification_colors = {"Offensive": "#FFD334", "Sexually Explicit": "#2D96FF", "Sexually Suggestive": "#FF5100", "Other": "#25D366", "Self Harm & Death": "#f77d07"}
category_bar_colors = {"Mental & Emotional Health": "rgb(255,211,52)", "Other Toxic Content": "rgb(45,150,255)", "Violence & Threats": "rgb(236,88,0)", "Cyberbullying": "rgb(37,211,102)", "Self Harm & Death": "rgb(255,81,0)", "Sexual & Inappropriate Content": "rgb(160,32,240)"}
image_location = {"Instagram": "assets/Instagram.png", "Twitter": "assets/twitter.png", "Facebook": "assets/facebook.png", "Tiktok": "assets/tiktok.png"}
platform_icons = {"Instagram": "skill-icons:instagram", "Twitter": "devicon:twitter", "Facebook": "devicon:facebook", "Tiktok": "logos:tiktok-icon"}


# Filter Functions
def time_filter(dataframe, time_value, date_range_value):
    if(time_value == "all"):
        end_date = datetime.combine(datetime.strptime(date_range_value[1], "%Y-%m-%d"), datetime.max.time())
        start_date = datetime.combine(datetime.strptime(date_range_value[0], "%Y-%m-%d"), datetime.min.time())
        dataframe["createTime_contents"] = pd.to_datetime(dataframe["createTime_contents"], format="%Y-%m-%d %H:%M:%S.%f")
        dataframe = dataframe[(dataframe["createTime_contents"] >= start_date) & (dataframe["createTime_contents"] <= end_date)]
        return dataframe
    else:
        end_date = datetime.combine(todays_date, datetime.max.time())
        if(time_value == "W"):
            start_date = end_date - timedelta(days=end_date.weekday())
        elif(time_value == "M"):
            start_date = end_date.replace(day=1)
        elif(time_value == "Q"):
            start_date = end_date.replace(day=1).replace(month=3*round((end_date.month - 1) // 3 + 1) - 2)
        elif(time_value == "A"):
            start_date = end_date.replace(day=1).replace(month=1)
        else:
            start_date = end_date
        start_date = datetime.combine(start_date, datetime.min.time())

        dataframe["createTime_contents"] = pd.to_datetime(dataframe["createTime_contents"], format="%Y-%m-%d %H:%M:%S.%f")
        dataframe = dataframe[(dataframe["createTime_contents"] >= start_date) & (dataframe["createTime_contents"] <= end_date)]
        return dataframe

def member_filter(dataframe, member_value):
    if((member_value is not None) and (member_value != "all")):
        dataframe = dataframe[dataframe["name_childrens"] == member_value]
    return dataframe

def platform_filter(dataframe, platform_value):
    if((platform_value is not None) and (platform_value != "all")):
        dataframe = dataframe[dataframe["platform_contents"] == platform_value]
    return dataframe

def alert_filter(dataframe, alert_value):
    if((alert_value is not None) and (alert_value != "all")):
        dataframe = dataframe[dataframe["alert_contents"] == alert_value]
    return dataframe

def slider_filter(dataframe, slider_value):
    start_date = pd.to_datetime(date_dict[slider_value[0]], format="%Y-%m-%d").date()
    end_date = pd.to_datetime(date_dict[slider_value[1]], format="%Y-%m-%d").date()
    dataframe["commentTime_comments"] = pd.to_datetime(dataframe["commentTime_comments"], format="%Y-%m-%d %H:%M:%S").dt.date
    dataframe = dataframe[(dataframe["commentTime_comments"] >= start_date) & (dataframe["commentTime_comments"] <= end_date)]
    return dataframe


# Function if No Data is available
def no_data_graph():
    message = html.Div(className="no_data_message", id="no_data_message", children=[
        html.Img(src="assets/images/empty_ghost.gif", width="40%"),
        html.P("No Data to Display", style={"fontSize": "24px", "color": "red", "margin": "0px"}),
        html.P("Please make a different Filter Selection", style={"fontSize": "18px", "color": "black", "margin": "0px"})
        ], style={"height": "100%"}
    )
    return message


# SideBar
sidebar = html.Div(id="sidebar", className="sidebar", children=[
    html.Div(children=[
        html.A(html.Div(className="sidebar_header", children=[
            html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/chatstatlogo.png"),
            html.H2("chatstat")
            ]), href="https://chatstat.com/", target="_blank", style={"color": "#25D366", "textDecoration": "none"}
        ),

        html.Div(id="sidebar_navlink_menu", className="sidebar_navlink_menu", children=[
            html.P("Main Menu", style={"margin": "0px 0.2rem", "font-family": "Poppins", "font-size": "12px", "font-weight": "500", "color": "white"}),
            html.Hr(style={"flex": "1", "border": "1px solid white", "borderRadius": "5px", "margin": "0px 0.5rem", "opacity": "unset"})
        ]),
        dbc.Nav(className="sidebar_navlink", children=[
            dbc.NavLink(children=[html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/dashboard.png"), html.Span("Dashboard")],
                        href="/dashboard", active="exact", className="sidebar_navlink_option"),
            dbc.NavLink(children=[html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/analytics.png"), html.Span("Analytics")],
                        href="/analytics", active="exact", className="sidebar_navlink_option"),
            dbc.NavLink(children=[html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/report.png"), html.Span("Report & Logs")],
                        href="/report", active="exact", className="sidebar_navlink_option")
            ],
        vertical=True, pills=True),

        html.Div(id="sidebar_navlink_menu", className="sidebar_navlink_menu", children=[
            html.P("General", style={"margin": "0px 0.2rem", "font-family": "Poppins", "font-size": "12px", "font-weight": "500", "color": "white"}),
            html.Hr(style={"flex": "1", "border": "1px solid white", "borderRadius": "5px", "margin": "0px 0.5rem", "opacity": "unset"})
        ]),
        dbc.Nav(className="sidebar_navlink", children=[
            dbc.NavLink(children=[html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/account.png"), html.Span("My Account")],
                        href="/account", active="exact", className="sidebar_navlink_option"),
            dbc.NavLink(children=[html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/setting.png"), html.Span("Settings")],
                        href="/settings", active="exact", className="sidebar_navlink_option")
            ],
        vertical=True, pills=True)
    ]),

    html.Img(id="sidebar_help", className="sidebar_help", src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/help_circle.png"),
    html.Div(id="sidebar_help_container", className="sidebar_help_container", children=[
        html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/help_circle.png", width="20%",
                 style={"position": "absolute", "top": "-15%", "padding": "5px", "border-radius": "100%", "background-color": "#25D366"}),
        html.P("Need Help?"),
        html.A(html.P("Go to Learning Centre", style={"padding": "0px 10px", "background-color": "#052F5F", "border-radius": "5px"}),
               href="https://chatstat.com/faq/", target="_blank", style={"color": "white", "textDecoration": "none"})
    ])
])


# Header
dashboard_header = dmc.Header(id="header", className="header", height="8.5vh", fixed=False, children=[
    dmc.Text("Dashboard", className="header_title"),
    dmc.Menu(id="user_container", className="user_container", trigger="hover", children=[
        dmc.MenuTarget(html.Div(id="user_information", className="user_information", children=[
            dmc.Avatar(id="user_avatar", className="user_avatar", src="assets/images/user.jpeg", size="6vh", radius="100%"),
            dmc.Text("Lawrence", id="user_name", className="user_name")
        ])),
        dmc.MenuDropdown(children=[
            dmc.MenuItem("My Account", icon=DashIconify(icon="material-symbols:account-box-outline", width=30), href="https://www.linkedin.com/company/chatstat/mycompany/", target="_blank"),
            dmc.MenuItem("Settings", icon=DashIconify(icon="lets-icons:setting-alt-line", width=30), href="https://www.linkedin.com/company/chatstat/mycompany/", target="_blank")
        ])
    ])
])

analytics_header = dmc.Header(id="header", className="header", height="8.5vh", fixed=False, children=[
    dmc.Text("Analytics", className="header_title"),
    dmc.Menu(id="user_container", className="user_container", trigger="hover", children=[
        dmc.MenuTarget(html.Div(id="user_information", className="user_information", children=[
            dmc.Avatar(id="user_avatar", className="user_avatar", src="assets/images/user.jpeg", size="6vh", radius="100%"),
            dmc.Text("Lawrence", id="user_name", className="user_name")
        ])),
        dmc.MenuDropdown(children=[
            dmc.MenuItem("My Account", icon=DashIconify(icon="material-symbols:account-box-outline", width=30), href="https://www.linkedin.com/company/chatstat/mycompany/", target="_blank"),
            dmc.MenuItem("Settings", icon=DashIconify(icon="lets-icons:setting-alt-line", width=30), href="https://www.linkedin.com/company/chatstat/mycompany/", target="_blank")
        ])
    ])
])


# Controls
filters = dmc.Group([
    dmc.Group(className="filter_container", children=[
        html.P("FILTERS", className="filter_label", id="filter_label"),
        dmc.HoverCard(openDelay=1200, position="right", transition="pop", children=[
            dmc.HoverCardTarget(
                dmc.SegmentedControl(id="time_control", className="time_control", value="A", radius="md", size="xs", data=[
                    {"label": "Daily", "value": "D"},
                    {"label": "Weekly", "value": "W"},
                    {"label": "Monthly", "value": "M"},
                    {"label": "Quarterly", "value": "Q"},
                    {"label": "Yearly", "value": "A"},
                    {"label": "Custom Range", "value": "all"}
                ])
            ),
            dmc.HoverCardDropdown(id="time_control_information", className="time_control_information")
        ]),
        dbc.Popover(id="popover_date_picker", className="popover_date_picker", children=[
            dbc.PopoverHeader("Selected Date Range", className="popover_date_picker_label"),
            dmc.DateRangePicker(id="date_range_picker", className="date_range_picker", clearable=False, inputFormat="MMM DD, YYYY",
                                icon=DashIconify(icon=f"arcticons:calendar-simple-{todays_date.day}", color="black", width=30),
                                value=[todays_date.date()-timedelta(days=60), todays_date.date()]
            )
            ], target="time_control", placement="bottom", trigger="legacy", hide_arrow=True
        ),
        html.Div(id="member_dropdown_container", className="member_dropdown_container", children=[
            html.P("Members", className="member_dropdown_label"),
            dmc.Select(className="member_dropdown", id="member_dropdown", clearable=False, searchable=False, value="all",
               rightSection=DashIconify(icon="radix-icons:chevron-down", color="black")
            ),
        ]),
        html.Div(className="platform_dropdown_container", children=[
            html.P("Social Platform", className="platform_dropdown_label"),
            dmc.Select(className="platform_dropdown", id="platform_dropdown", clearable=False, searchable=False, value="all",
                rightSection=DashIconify(icon="radix-icons:chevron-down", color="black")
            )
        ]),
        html.Div(className="alert_dropdown_container", children=[
            html.P("Alert Level", className="alert_dropdown_label"),
            dmc.Select(className="alert_dropdown", id="alert_dropdown", clearable=False, searchable=False, value="all",
                rightSection=DashIconify(icon="radix-icons:chevron-down", color="black")
            )
        ]),
        dmc.ActionIcon(DashIconify(icon="grommet-icons:power-reset", color="white", width=25, flip="horizontal"), id="reset_filter_container", className="reset_filter_container", n_clicks=0, variant="transparent")
    ], spacing="10px"
    ),
    html.Div(className="searchbar_container", id="searchbar_container", children=[
        html.P("Member Overview", className="searchbar_label", id="searchbar_label"),
        dmc.Select(className="searchbar", id="searchbar", clearable=True, searchable=True, placeholder=" Search...", nothingFound="Nothing Found", limit=5, iconWidth=50,
               icon=html.Img(src="https://chatstat-dashboard.s3.ap-southeast-2.amazonaws.com/images/chatstatlogo_black.png", width="50%"), rightSection=DashIconify(icon="radix-icons:chevron-right", color="black"),
               data=[{"group": "Members", "label": member.title(), "value": member} for member in list(df["name_childrens"].unique())] +
                    [{"group": "ID", "label": id.title(), "value": id} for id in list(df["id_childrens"].unique())]
        )
    ])
], style={"margin": "10px"}, spacing="10px"
)


# Overview Card
overview = html.Div(children=[
    dmc.Modal(title="Member Overview", id="child_overview", zIndex=10000, centered=True, overflow="inside", children=[
        dmc.Group(children=[
            dmc.Avatar(id="user_avatar", className="user_avatar", src="assets/images/user.jpeg", size=100, radius="100%"),
            html.P(children=[
                html.Strong("Name: "), html.Span(), html.Br(),
                html.Strong("Email: "), html.Span(), html.Br(),
                html.Strong("ID: "), html.Span()
            ])
        ]),
        html.Div(id="overview_platform")
    ]),
    dmc.Button("Open modal", id="button")
])


# KPI Card
kpi_cards = html.Div([
    dmc.Card(id="kpi_alert_count_container", className="kpi_alert_count_container", withBorder=True, radius="5px", style={"width": "auto", "margin": "0px 10px 0px 0px"}),
    html.Div(id="kpi_platform_count", className="kpi_platform_count")
    ], style={"display": "flex", "flexDirection": "row", "margin": "10px", "padding": "0px"}
)


# Page Charts
dashboard_charts = html.Div(children=[
    html.Div(id="row1", className="row1", children=[
        html.Div(id="content_risk_classification_container", className="content_risk_classification_container", children=[
            html.Div(id="content_classification_radial_chart", className="content_classification_radial_chart"),
            html.Div(id="risk_categories_horizontal_bar", className="risk_categories_horizontal_bar"),
            dmc.ActionIcon(DashIconify(icon="f7:camera-fill", color="rgba(68, 68, 68, 0.3)", width=16), id="save_as_image", className="save_as_image", n_clicks=0, variant="transparent")
            ], style={"width": "calc(65% - 5px)"}
        ),
        html.Div(id="content_risk_bar_chart", className="content_risk_bar_chart", style={"width": "calc(35% - 5px)"})
    ], style={"margin": "10px", "padding": "0px"}
    ),
    html.Div(id="row2", className="row2", children=[
        html.Div(id="comment_alert_line_chart_container", className="comment_alert_line_chart_container", children=[
            html.Div(id="comment_alert_line_chart"),
            html.Div(dcc.RangeSlider(id="comment_alert_line_chart_slider", className="comment_alert_line_chart_slider", updatemode="drag", pushable=1, min=0, max=730, value=[0, 730]), style={"height": "50px"})
            ], style={"width": "calc(65% - 5px)"}
        ),
        html.Div(id="comment_classification_pie_chart", className="comment_classification_pie_chart", style={"width": "calc(35% - 5px)"})
    ], style={"margin": "10px", "padding": "0px"}
    )
], style={"height": "100%", "width": "100%", "margin": "0px", "padding": "0px"})

analytics_charts = html.Div(children=[
    dcc.Graph(id="content_classification_sunburst_chart", config=plot_config),
    dcc.Graph(id="content_result_treemap"),
    dcc.Graph(id="comment_result_radar", style={"width": "100%"}),
    dcc.Graph(id="content_result_bubble_chart", style={"width": "100%"}),
])


# Designing Main App
app = Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=["https://fonts.googleapis.com/css2?family=Poppins:wght@200;300;400;500;600;700&display=swap", dbc.themes.BOOTSTRAP, dbc.themes.MATERIA, dbc.icons.FONT_AWESOME])
server = app.server
app.css.config.serve_locally = True
app.title = "Parent Dashboard"
app.layout = html.Div(
    children=[
        dcc.Interval(id="time_interval", disabled=True),
        dcc.Location(id="url_path", refresh=False),
        html.Div(children=[], style={"height": "8.5vh"}),
        html.Div(children=[], style={"width": "4.5rem", "display": "inline-block"}),
        html.Div(id="page_content", style={"display": "inline-block", "width": "calc(100% - 4.5rem)"})
    ]
)


# Website Page Navigation
@app.callback(Output("page_content", "children"),
              [Input("url_path", "pathname")]
)
def display_page(pathname):
    if pathname == "/dashboard":
        return [sidebar, dashboard_header, filters, kpi_cards, dashboard_charts]
    elif pathname == "/analytics":
        return [sidebar, analytics_header, filters, analytics_charts]
    elif pathname == "/report":
        return [sidebar]
    else:
        return [sidebar, overview]


# Time Control Information
@app.callback(
    Output("time_control_information", "children"),
    Input("time_interval", "n_intervals")
)
def update_time_control_information(time_interval):
    information = [
        DashIconify(icon="ion:information-circle-outline", color="#0b71aa", width=30, style={"position": "absolute", "top": "10px", "right": "10px"}),
        html.P(children=[
            html.Strong("Daily: "), html.Span(f"For Today's Date {todays_date.strftime('%d %b, %Y')}"), html.Br(),
            html.Strong("Weekly: "), html.Span(f"From Monday to Sunday"), html.Br(),
            html.Strong("Monthly: "), html.Span(f"From the 1st of {todays_date.strftime('%B')}"), html.Br(),
            html.Strong("Quarterly: "), html.Span(f"For this Quarter starting from {todays_date.replace(month=3*round((todays_date.month - 1) // 3 + 1) - 2).strftime('%B')}"), html.Br(),
            html.Strong("Yearly: "), html.Span(f"From the Beginning of {todays_date.year}"), html.Br(),
            html.Strong("Custom Range: "), html.Span("Select from Date Picker")
        ])
    ]
    return information


# Date Picker
@app.callback(
    [Output("popover_date_picker", "style"), Output("popover_date_picker", "offset")],
    [Input("time_control", "value")]
)
def update_popover_date_picker(time_value):
    if(time_value == "all"):
        return {"display": "block"}, "160,10"
    else:
        return {"display": "none"}, ""


# Member Dropdown
@app.callback(
    [Output("member_dropdown", "data"), Output("member_dropdown", "icon"), Output("member_dropdown_container", "style")],
    Input("member_dropdown", "value")
)
def update_member_dropdown(member_value):
    user_list = df["name_childrens"].unique()
    if(len(user_list) == 1):
        style_flag = "none"
        data = [{"value": "all", "label": i.split(" ")[0].title()} for i in user_list if ((str(i).lower() != "nan") and (str(i).lower() != "no"))]
    else:
        style_flag = "block"
        data = [{"value": "all", "label": "All Members"}] + [{"value": i, "label": i.split(" ")[0].title()} for i in user_list if ((str(i).lower() != "nan") and (str(i).lower() != "no"))]
    return data, DashIconify(icon=f"tabler:square-letter-{member_value[0].lower()}", width=25, color="#25D366"), {"display": style_flag}


# Platform Dropdown
@app.callback(
    [Output("platform_dropdown", "data"), Output("platform_dropdown", "icon")],
    Input("platform_dropdown", "value")
)
def update_platform_dropdown(platform_value):
    platform_list = df["platform_contents"].unique()
    data = [{"label": "All Platforms", "value": "all"}] + [{"label": i.title(), "value": i} for i in platform_list if ((str(i).lower() != "nan") and (str(i).lower() != "no"))]
    if(platform_value in ["all", None]):
        return data, DashIconify(icon="emojione-v1:globe-showing-asia-australia", width=20)
    else:
        return data, DashIconify(icon=platform_icons[platform_value.title()], width=20)


# Alert Dropdown
@app.callback(
    [Output("alert_dropdown", "data"), Output("alert_dropdown", "icon")],
    Input("alert_dropdown", "value")
)
def update_alert_dropdown(alert_value):
    alert_list = df["alert_contents"].unique()
    data = [{"label": "All Alerts", "value": "all"}] + [{"label": i.title(), "value": i}
                for i in sorted(alert_list, key=lambda x: ["high", "medium", "low"].index(x.lower())
                    if isinstance(x, str) and x.lower() in ["high", "medium", "low"] else float("inf"))
                    if (isinstance(i, str) and str(i).lower() != "nan") and (str(i).lower() != "no")]
    if(alert_value in ["all", None]):
        return data, DashIconify(icon="line-md:alert", color="#012749", width=30)
    else:
        return data, DashIconify(icon="line-md:alert", color=alert_colors[alert_value.title()], width=30)


# Reset Filter Container
@app.callback(
    [Output("time_control", "value"), Output("member_dropdown", "value"), Output("platform_dropdown", "value"), Output("alert_dropdown", "value")],
    Input("reset_filter_container", "n_clicks"),
    prevent_initial_call=True
)
def reset_filters(n_clicks):
    return "A", "all", "all", "all"


# Generate Overview Card
@app.callback(
    [Output("child_overview", "opened"), Output("overview_platform", "children")],
    Input("button", "n_clicks"),
    prevent_initial_call=True
)
def update_overview_card(n_clicks):
    overview_platform_df = df.copy()
    overview_platform_df = overview_platform_df[(overview_platform_df["alert_contents"].str.lower() != "no") & (overview_platform_df["alert_contents"].str.lower() != "") & (overview_platform_df["alert_contents"].notna())]

    overview_platform_df["createTime_contents"] = pd.to_datetime(overview_platform_df["createTime_contents"], format="%Y-%m-%d %H:%M:%S.%f")
    overview_platform_df = overview_platform_df.groupby(by=["platform_contents"], as_index=False)["id_contents"].nunique()
    overview_platform_df.columns = ["platform", "count"]
    overview_platform_df["percentage_count"] = (overview_platform_df["count"]/overview_platform_df["count"].sum()) * 100
    print(overview_platform_df)

    platform_ring = dmc.RingProgress(size=130, thickness=12,
        sections=[{"value": row["percentage_count"], "color": platform_colors[row["platform"]]} for index, row in overview_platform_df.iterrows()],
        label=dmc.Center(html.P(children=[html.Strong(overview_platform_df["count"].sum()), html.Br(), html.Strong("Alerts")],
                                style={"text-align": "center"}))
    )
    return True, platform_ring


# KPI Count Card
@app.callback(
    Output("kpi_alert_count_container", "children"),
    [Input("time_control", "value"), Input("date_range_picker", "value"), Input("member_dropdown", "value")]
)
def update_kpi_count(time_value, date_range_value, member_value):
    alert_count_df = df.copy()
    alert_count_df = alert_count_df[(alert_count_df["alert_contents"].str.lower() != "no") & (alert_count_df["alert_contents"].str.lower() != "") & (alert_count_df["alert_contents"].notna())]

    # Filters
    alert_count_df = member_filter(alert_count_df, member_value)
    if(time_value == "all"):
        alert_count_df = time_filter(alert_count_df, time_value, date_range_value)
        card = [
            dmc.Text("Number of Alerts", id="kpi_alert_count_label", className="kpi_alert_count_label"),
            html.Div(id="kpi_alert_count", className="kpi_alert_count", children=[
                dmc.Group(children=[
                    dmc.Text(alert_count_df["id_contents"].nunique(), style={"color": "#052F5F", "fontSize": "40px", "fontFamily": "Poppins", "fontWeight": 600}),
                    dmc.Stack([
                        dmc.Text("Between ", color="dimmed", style={"fontSize": "12px", "fontFamily": "Poppins", "text-align": "right"}),
                        dmc.Text(datetime.strptime(date_range_value[0], "%Y-%m-%d").strftime("%b %d, %Y"), color="dimmed", style={"fontSize": "12px", "fontFamily": "Poppins", "text-align": "right"}),
                        dmc.Text("& " + datetime.strptime(date_range_value[1], "%Y-%m-%d").strftime("%b %d, %Y"), color="dimmed", style={"fontSize": "12px", "fontFamily": "Poppins", "text-align": "right"})
                        ], align="center", justify="center", spacing="0px")
                ], position="center", style={"margin": "0px", "padding": "0px"}),
            ])
        ]
        return card

    else:
        alert_count_df["createTime_contents"] = pd.to_datetime(alert_count_df["createTime_contents"], format="%Y-%m-%d %H:%M:%S.%f")
        alert_count_df.set_index("createTime_contents", inplace=True)
        alert_count_df = alert_count_df.resample(time_value)["id_contents"].nunique()
        alert_count_df = alert_count_df.reset_index()
        alert_count_df.columns = ["date", "count"]
        if(len(alert_count_df) == 1):
            alert_count_df["increase"] = alert_count_df["count"].diff().fillna(alert_count_df["count"].iloc[0]).astype(int)
        else:
            alert_count_df["increase"] = alert_count_df["count"].diff().fillna(0).astype(int)
        alert_count_df["date"] = alert_count_df["date"].dt.date
        alert_count_df = alert_count_df.sort_values(by="date", ascending=False)

        if(time_value == "W"):
            today = todays_date.date()
            date_comparison = today + timedelta(days=(6 - today.weekday()) % 7)
            metric_text = "vs Last Week"
        elif(time_value == "M"):
            today = todays_date.date()
            date_comparison = datetime(today.year, today.month, calendar.monthrange(today.year, today.month)[-1]).date()
            metric_text = "vs Last Month"
        elif(time_value == "Q"):
            today = todays_date.date()
            date_comparison = (today + pd.tseries.offsets.QuarterEnd(0)).date()
            metric_text = "vs Last Quarter"
        elif(time_value == "A"):
            today = todays_date.date()
            date_comparison = datetime(today.year, 12, 31).date()
            metric_text = "vs Last Year"
        else:
            date_comparison = todays_date.date()
            metric_text = "vs Last Day"

        if(date_comparison in alert_count_df["date"].values):
            card = [
                dmc.Text("Number of Alerts", id="kpi_alert_count_label", className="kpi_alert_count_label"),
                html.Div(id="kpi_alert_count", className="kpi_alert_count", children=[
                    dmc.Group(children=[
                        dmc.Text(alert_count_df["count"].iloc[0], style={"color": "#052F5F", "fontSize": "40px", "fontFamily": "Poppins", "fontWeight": 600}),
                        dmc.Stack([
                            dmc.Text("▲"+str(alert_count_df["increase"].iloc[0]) if alert_count_df["increase"].iloc[0] > 0 else "▼"+str(abs(alert_count_df["increase"].iloc[0])),
                                     style={"color": "#FF5100" if alert_count_df["increase"].iloc[0] > 0 else "#25D366", "fontSize": "20px", "fontFamily": "Poppins", "fontWeight": 600}),
                            dmc.Text(metric_text, color="dimmed", style={"fontSize": "12px", "fontFamily": "Poppins", "text-align": "right"})
                            ], align="center", justify="center", spacing="0px")
                    ], position="center", style={"margin": "0px", "padding": "0px"}),
                ])
            ]
        else:
            card = [
                dmc.Text("Number of Alerts", id="kpi_alert_count_label", className="kpi_alert_count_label"),
                html.Div(id="kpi_alert_count", className="kpi_alert_count", children=[
                    dmc.Text("No Data Found", color="black", style={"fontSize": 17, "fontFamily": "Poppins", "fontWeight": "bold", "text-align": "center"})
                ])
            ]
        return card


# KPI Platform Card
@app.callback(
    Output("kpi_platform_count", "children"),
    [Input("time_control", "value"), Input("date_range_picker", "value"), Input("member_dropdown", "value"), Input("alert_dropdown", "value")]
)
def update_kpi_platform(time_value, date_range_value, member_value, alert_value):
    kpi_platform_df = df.copy()
    kpi_platform_df = kpi_platform_df[(kpi_platform_df["alert_contents"].str.lower() != "no") & (kpi_platform_df["alert_contents"].str.lower() != "") & (kpi_platform_df["alert_contents"].notna())]
    kpi_platform_df = kpi_platform_df[(kpi_platform_df["result_contents"].str.lower() != "no") & (kpi_platform_df["result_contents"].str.lower() != "") & (kpi_platform_df["result_contents"].notna())]

    # Filters
    kpi_platform_df = time_filter(kpi_platform_df, time_value, date_range_value)
    kpi_platform_df = member_filter(kpi_platform_df, member_value)
    kpi_platform_df = alert_filter(kpi_platform_df, alert_value)

    if(len(kpi_platform_df) == 0):
        card = dmc.Card(id="kpi_platform_count_elements", className="kpi_platform_count_elements", children=[
            dmc.Text("No Cards to Display", color="black", style={"fontSize": 17, "fontFamily": "Poppins", "fontWeight": "bold"})
            ], withBorder=True, radius="5px", style={"height": "100%", "width": "100%", "display": "flex", "justify-content": "center", "align-items": "center"}
        )
        return card
    else:
        kpi_platform_df = kpi_platform_df.groupby(by=["platform_contents", "result_contents"], as_index=False)["id_contents"].nunique()
        kpi_platform_df.columns = ["platform", "result", "count"]
        kpi_platform_df.sort_values(by=["platform", "count"], ascending=[True, False], inplace=True)

        kpi_list = []
        for platform in kpi_platform_df["platform"].unique():

            # Producing Increase for each Platform
            if(time_value != "all"):
                kpi_platform_count_df = df.copy()
                kpi_platform_count_df = kpi_platform_count_df[(kpi_platform_count_df["alert_contents"].str.lower() != "no") & (kpi_platform_count_df["alert_contents"].str.lower() != "") & (kpi_platform_count_df["alert_contents"].notna())]
                kpi_platform_count_df = kpi_platform_count_df[(kpi_platform_count_df["result_contents"].str.lower() != "no") & (kpi_platform_count_df["result_contents"].str.lower() != "") & (kpi_platform_count_df["result_contents"].notna())]
                kpi_platform_count_df["createTime_contents"] = pd.to_datetime(kpi_platform_count_df["createTime_contents"], format="%Y-%m-%d %H:%M:%S.%f")

                # Filters
                kpi_platform_count_df = kpi_platform_count_df[kpi_platform_count_df["platform_contents"] == platform]
                kpi_platform_count_df = member_filter(kpi_platform_count_df, member_value)
                kpi_platform_count_df = alert_filter(kpi_platform_count_df, alert_value)

                kpi_platform_count_df.set_index("createTime_contents", inplace=True)
                kpi_platform_count_df = kpi_platform_count_df.resample(time_value)["id_contents"].nunique()
                kpi_platform_count_df = kpi_platform_count_df.reset_index()
                kpi_platform_count_df.columns = ["date", "count"]
                if(len(kpi_platform_count_df) == 1):
                    kpi_platform_count_df["increase"] = kpi_platform_count_df["count"].diff().fillna(kpi_platform_count_df["count"].iloc[0]).astype(int)
                else:
                    kpi_platform_count_df["increase"] = kpi_platform_count_df["count"].diff().fillna(0).astype(int)
                kpi_platform_count_df["date"] = kpi_platform_count_df["date"].dt.date
                kpi_platform_count_df = kpi_platform_count_df.sort_values(by="date", ascending=False)

            platform_df = kpi_platform_df[kpi_platform_df["platform"] == platform]
            title = platform.title()+f" - {alert_value} Alerts" if ((alert_value is not None) and (alert_value != "all")) else platform.title()
            kpi_list.append(
                dmc.Card(id="kpi_platform_count_elements", className="kpi_platform_count_elements", children=[
                    dbc.Row([
                        dbc.Col(dmc.Text(title, style={"color": "black", "fontSize": "18px", "fontFamily": "Poppins", "fontWeight": "bold"}),
                        align="center", width=9),
                        dbc.Col(dmc.Text(id="kpi_platform_comparison", className="kpi_platform_comparison", children=["--" if time_value == "all"
                                        else ("▲"+str(kpi_platform_count_df["increase"].iloc[0]) if kpi_platform_count_df["increase"].iloc[0] > 0
                                              else "▼"+str(abs(kpi_platform_count_df["increase"].iloc[0])))],
                                    style={"color": "#25D366" if time_value == "all" else ("#FF5100" if kpi_platform_count_df["increase"].iloc[0] > 0 else "#25D366"),
                                           "background-color": "rgba(37, 211, 102, 0.3)" if time_value == "all" else ("rgba(255, 81, 0, 0.3)" if kpi_platform_count_df["increase"].iloc[0] > 0 else "rgba(37, 211, 102, 0.3)")}),
                        align="center", width="auto")
                    ], justify="between"),
                    dbc.Row([
                        dbc.Col(html.Img(id="kpi_platform_image", className="kpi_platform_image", src=f"assets/images/{platform}.png"),
                        align="center", width=2, style={"padding": "0px 0px 0px 12px"}),
                        dbc.Col(dmc.Stack(children=[
                            html.Div(children=[
                                dmc.Text(row["result"], style={"color": "#979797", "fontSize": "12px", "fontFamily": "Poppins"}),
                                dmc.Text(row["count"], style={"color": "#052F5F", "fontSize": "12px", "fontFamily": "Poppins", "fontWeight": "bold"})
                            ], style={"display": "flex", "justifyContent": "space-between", "width": "100%"})
                            for index, row in platform_df.iterrows()], align="flex-start", justify="flex-end", spacing="0px"),
                        align="center", width=8),
                        dbc.Col(html.Div(children=[
                            dmc.Text(str(platform_df["count"].sum()), style={"color": "#052F5F", "fontSize": "40px", "fontFamily": "Poppins", "fontWeight": 600})
                            ], style={"text-align": "right"}),
                        align="center", width=2)
                    ], justify="between")
                ], withBorder=True, radius="5px", style={"flex": 1, "height": "100%"}
                )
            )
        return kpi_list


# Content Classification Radial Chart
@app.callback(
    Output("content_classification_radial_chart", "children"),
    [Input("time_control", "value"), Input("date_range_picker", "value"), Input("member_dropdown", "value"), Input("platform_dropdown", "value"), Input("alert_dropdown", "value")]
)
def update_radial_chart(time_value, date_range_value, member_value, platform_value, alert_value):
    result_contents_df = df.copy()
    result_contents_df = result_contents_df[(result_contents_df["result_contents"].str.lower() != "no") & (result_contents_df["result_contents"].str.lower() != "") & (result_contents_df["result_contents"].notna())]
    result_contents_df = result_contents_df[(result_contents_df["alert_contents"].str.lower() != "no") & (result_contents_df["alert_contents"].str.lower() != "") & (result_contents_df["alert_contents"].notna())]

    # Filters
    result_contents_df = time_filter(result_contents_df, time_value, date_range_value)
    result_contents_df = member_filter(result_contents_df, member_value)
    result_contents_df = platform_filter(result_contents_df, platform_value)
    result_contents_df = alert_filter(result_contents_df, alert_value)

    if(len(result_contents_df) == 0):
        return no_data_graph()
    else:
        result_contents_df = result_contents_df.groupby(by=["result_contents"], as_index=False)["id_contents"].nunique()
        result_contents_df.columns = ["classification", "count"]
        result_contents_df["radial"] = (result_contents_df["count"] / result_contents_df["count"].sum()) * 270
        result_contents_df["total_radial"] = 270
        result_contents_df.sort_values(by=["radial"], ascending=True, inplace=True)

        if(((platform_value is not None) and (platform_value != "all")) and ((alert_value is not None) and (alert_value != "all"))):
            title = f"Comment Risk Classification - {platform_value} & {alert_value} Alerts"
        elif((platform_value is not None) and (platform_value != "all")):
            title = f"Content Risk Classification - {platform_value}"
        elif((alert_value is not None) and (alert_value != "all")):
            title = f"Content Risk Classification - {alert_value} Alerts"
        else:
            title = "Content Risk Classification"
        return [
            html.P(title, style={"color": "#052F5F", "fontWeight": "bold", "fontSize": 17, "margin": "10px 25px 0px 25px"}),
            html.Div(id="content_classification_image", className="content_classification_image", children=[
                html.Img(src=radial_bar_chart.radial_chart(result_contents_df), width="100%", style={"object-fit": "cover"})]
            )
        ]


# Risk Categories Horizontal Bar
@app.callback(
    Output("risk_categories_horizontal_bar", "children"),
    [Input("time_control", "value"), Input("date_range_picker", "value"), Input("member_dropdown", "value")]
)
def update_horizontal_bar(time_value, date_range_value, member_value):
    risk_categories_df = df.copy()
    risk_categories_df = risk_categories_df[(risk_categories_df["result_contents"].str.lower() != "no") & (risk_categories_df["result_contents"].str.lower() != "") & (risk_categories_df["result_contents"].notna())]
    risk_categories_df = risk_categories_df[(risk_categories_df["alert_contents"].str.lower() != "no") & (risk_categories_df["alert_contents"].str.lower() != "") & (risk_categories_df["alert_contents"].notna())]

    # Filters
    risk_categories_df = time_filter(risk_categories_df, time_value, date_range_value)
    risk_categories_df = member_filter(risk_categories_df, member_value)

    if(len(risk_categories_df) == 0):
        return no_data_graph()
    else:
        risk_categories_df = risk_categories_df.groupby(by=["result_contents"], as_index=False)["id_contents"].nunique()
        risk_categories_df.columns = ["category", "count"]
        risk_categories_df["percentage_of_total"] = (risk_categories_df["count"] / risk_categories_df["count"].sum()) * 100
        risk_categories_df["percentage_of_total"] = risk_categories_df["percentage_of_total"].round().astype(int)
        risk_categories_df.loc[risk_categories_df["percentage_of_total"].idxmax(), "percentage_of_total"] += 100 - risk_categories_df["percentage_of_total"].sum()

        bar_sections = []
        risk_categories_df.sort_values(by="percentage_of_total", ascending=True, inplace=True)
        for index, row in risk_categories_df.iterrows():
            bar_sections.append({"value": row["percentage_of_total"], "color": category_bar_colors[row["category"]], "label": str(row["percentage_of_total"])+"%"})

        # Handling Missing Values
        for classification in category_bar_colors.keys():
            if(classification not in risk_categories_df["category"].unique()):
                new_row = {"category": classification, "count": 0, "percentage_of_total": 0}
                risk_categories_df = pd.concat([risk_categories_df, pd.DataFrame(new_row, index=[len(risk_categories_df)])])
        
        bar_legend = []
        risk_categories_df.sort_values(by="percentage_of_total", ascending=False, inplace=True)
        for index, row in risk_categories_df.iterrows():
            bar_legend.append([
                dmc.Col(DashIconify(className="risk_categories_progress_legend_symbol", icon="material-symbols:circle", width=12, color=category_bar_colors[row["category"]]), span=1),
                dmc.Col(dmc.Text(className="risk_categories_progress_legend_marking", children=row["category"]), span=6),
                dmc.Col(html.Header(row["count"], style={"color": "#081A51", "fontFamily": "Poppins", "fontWeight": "bold", "fontSize": 14, "text-align": "right"}), span=2),
                dmc.Col(dmc.Avatar(className="risk_categories_progress_legend_avatar", children=str(row["percentage_of_total"])+"%", size=35, radius="xl", color="#2D96FF"), span=1, offset=2)
                ]
            )

        return dmc.Stack(className="risk_categories_progress_bar_container", id="risk_categories_progress_bar_container", children=[
            html.Div(className="risk_categories_progress_bar_label", children=[
                html.Hr(style={"width": "30%", "borderTop": "2px solid", "borderBottom": "2px solid", "opacity": "unset"}),
                dmc.Text("Categories", style={"fontFamily": "Poppins"}),
                html.Hr(style={"width": "30%", "borderTop": "2px solid", "borderBottom": "2px solid", "opacity": "unset"})
            ], style={"width": "100%"}),
            dmc.Progress(className="risk_categories_progress_bar", sections=bar_sections, radius=10, size=25, animate=False, striped=False, style={"width": "95%"}),
            dmc.Grid(className="risk_categories_progress_legend", children=sum(bar_legend, []), gutter="xs", justify="center", align="center")
            ], justify="space-evenly")


# Content Risk Bar Chart
@app.callback(
    Output("content_risk_bar_chart", "children"),
    [Input("time_control", "value"), Input("date_range_picker", "value"), Input("member_dropdown", "value"), Input("platform_dropdown", "value")]
)
def update_bar_chart(time_value, date_range_value, member_value, platform_value):
    risk_content_df = df.copy()
    risk_content_df = risk_content_df[(risk_content_df["alert_contents"].str.lower() != "no") & (risk_content_df["alert_contents"].str.lower() != "") & (risk_content_df["alert_contents"].notna())]

    # Filters
    risk_content_df = time_filter(risk_content_df, time_value, date_range_value)
    risk_content_df = member_filter(risk_content_df, member_value)
    risk_content_df = platform_filter(risk_content_df, platform_value)

    if(len(risk_content_df) == 0):
        return no_data_graph()
    else:
        risk_content_df["createTime_contents"] = pd.to_datetime(risk_content_df["createTime_contents"], format="%Y-%m-%d %H:%M:%S.%f")
        risk_content_df = risk_content_df.groupby(by=["alert_contents", "platform_contents"], as_index=False)["id_contents"].nunique()
        risk_content_df.columns = ["alert", "platform", "count"]
        categories = ["High", "Medium", "Low"]

        # Handling Missing Values
        for platform in risk_content_df["platform"].unique():
            for cat in categories:
                if(cat not in risk_content_df[risk_content_df["platform"] == platform]["alert"].unique()):
                    new_row = {"alert": cat, "platform": platform.title(), "count": 0}
                    risk_content_df = pd.concat([risk_content_df, pd.DataFrame(new_row, index=[len(risk_content_df)])])

        risk_content_df["alert"] = pd.Categorical(risk_content_df["alert"], categories=categories, ordered=True)
        risk_content_df["percentage_alert"] = risk_content_df.groupby("alert")["count"].transform(lambda x: (x / x.sum()) * 100).round()
        risk_content_df["percentage_total"] = ((risk_content_df["count"] / risk_content_df["count"].sum()) * 100).round()
        risk_content_df = risk_content_df.sort_values(by="alert")

        if((platform_value is None) or (platform_value == "all")):
            content_risk = px.bar(risk_content_df, x="alert", y="count", text="count", hover_name="platform", custom_data=["percentage_total"], color="platform", color_discrete_map=platform_colors)
            content_risk.update_layout(title="<b>Alerts on User Content</b>", title_font_color="#052F5F", title_font=dict(size=17, family="Poppins"))
            content_risk.update_traces(width=0.5, hovertemplate="<i><b>%{hovertext}</b></i><br>Alert Severity: <b>%{x}</b><br>Total Alerts: <b>%{text}</b><br>% of Total: <b>%{customdata}%</b><extra></extra>")
        else:
            content_risk = px.bar(risk_content_df, x="alert", y="count", color="alert", color_discrete_map=alert_colors)
            content_risk.update_layout(title=f"<b>Alerts on User Content - {platform_value}</b>", title_font_color="#052F5F", title_font=dict(size=17, family="Poppins"))
            content_risk.update_traces(width=0.4, hovertemplate="Alert Severity: <b>%{x}</b><br>Total Alerts: <b>%{y}</b><extra></extra>")

        content_risk.update_layout(margin=dict(l=25, r=25, b=0), barmode="relative")
        content_risk.update_layout(legend=dict(font=dict(family="Poppins"), traceorder="grouped", orientation="h", x=1, y=1, xanchor="right", yanchor="bottom", title_text="", bgcolor="rgba(0,0,0,0)"))
        content_risk.update_layout(xaxis_title="", yaxis_title="", legend_title_text="", plot_bgcolor="rgba(0, 0, 0, 0)")
        content_risk.update_layout(yaxis_showgrid=True, yaxis_ticksuffix=" ", yaxis=dict(tickfont=dict(size=12, family="Poppins", color="#8E8E8E"), griddash="dash", gridwidth=1, gridcolor="#DADADA"))
        content_risk.update_layout(xaxis_showgrid=False, xaxis=dict(tickfont=dict(size=16, family="Poppins", color="#052F5F")))
        content_risk.update_layout(hoverlabel=dict(bgcolor="#c1dfff", font_size=12, font_family="Poppins", align="left"))
        content_risk.update_traces(marker_line=dict(color="black", width=1.5), textfont=dict(color="#052F5F", size=16, family="Poppins"), textangle=0)
        content_risk.update_xaxes(fixedrange=True)
        content_risk.update_yaxes(fixedrange=True)

        # Adding Bubbles on top for effect
        if(platform_value not in [None, "all"]):
            risk_content_df = risk_content_df.groupby(by=["alert"], as_index=False)["count"].sum()
            scatter_trace = px.scatter(risk_content_df, x="alert", y="count", color="alert", color_discrete_map=alert_colors, text="count")
            scatter_trace.update_layout(hoverlabel=dict(bgcolor="#c1dfff", font_size=12, font_family="Poppins", align="left"))
            scatter_trace.update_traces(hovertemplate="Alert Severity: <b>%{x}</b><br>Total Alerts: <b>%{y}</b><extra></extra>")
            scatter_trace.update_traces(textfont=dict(color="#052F5F", size=16, family="Poppins"))
            scatter_trace.update_traces(marker=dict(size=70, symbol="circle", line=dict(width=2, color="black")), showlegend=False)
            content_risk.add_trace(scatter_trace.data[0])
            content_risk.add_trace(scatter_trace.data[1])
            content_risk.add_trace(scatter_trace.data[2])

        return dcc.Graph(figure=content_risk, responsive=True, config=plot_config, style={"height": "100%", "width": "100%"})


# Comment Alert Line Chart
@app.callback(
    Output("comment_alert_line_chart", "children"),
    [Input("member_dropdown", "value"), Input("alert_dropdown", "value"), Input("comment_alert_line_chart_slider", "value")]
)
def update_line_chart(member_value, alert_value, slider_value):
    alert_comment_df = df.copy()
    alert_comment_df = alert_comment_df[(alert_comment_df["alert_comments"].str.lower() != "no") & (alert_comment_df["alert_comments"].str.lower() != "") & (alert_comment_df["alert_comments"].notna())]

    # Filters
    alert_comment_df = member_filter(alert_comment_df, member_value)
    alert_comment_df = alert_filter(alert_comment_df, alert_value)
    alert_comment_df = slider_filter(alert_comment_df, slider_value)

    if(len(alert_comment_df) == 0):
        return no_data_graph()
    else:
        alert_comment_df["commentTime_comments"] = pd.to_datetime(alert_comment_df["commentTime_comments"], format="%Y-%m-%d").dt.strftime("%b %Y")
        alert_comment_df = alert_comment_df.groupby(by=["commentTime_comments", "platform_comments"], as_index=False)["id_contents"].nunique()
        alert_comment_df.columns = ["commentTime", "platform", "count"]
        alert_comment_df["commentTime"] = pd.to_datetime(alert_comment_df["commentTime"], format="%b %Y")
        alert_comment_df.sort_values(by="commentTime", inplace=True)

        comment_alert = px.line(alert_comment_df, x="commentTime", y="count", hover_name="platform", color="platform", color_discrete_map=platform_colors)
        comment_alert.update_layout(margin=dict(l=25, r=25, b=0), height=400)
        comment_alert.update_layout(legend=dict(font=dict(family="Poppins"), traceorder="grouped", orientation="h", x=1, y=1, xanchor="right", yanchor="bottom", title_text=""))
        comment_alert.update_layout(xaxis_title="", yaxis_title="", legend_title_text="", plot_bgcolor="rgba(0, 0, 0, 0)")
        comment_alert.update_layout(yaxis_showgrid=True, yaxis_ticksuffix="  ", yaxis=dict(tickfont=dict(size=12, family="Poppins", color="#8E8E8E"), griddash="dash", gridwidth=1, gridcolor="#DADADA"))
        comment_alert.update_layout(xaxis_showgrid=False, xaxis=dict(tickfont=dict(size=9, family="Poppins", color="#052F5F"), tickangle=0))
        comment_alert.update_traces(mode="lines+markers", line=dict(width=2), marker=dict(sizemode="diameter", size=8, color="white", line=dict(width=2)))
        comment_alert.update_xaxes(fixedrange=True)
        comment_alert.update_yaxes(fixedrange=True)
        comment_alert.add_vline(x=alert_comment_df[alert_comment_df["count"] == alert_comment_df["count"].max()]["commentTime"].iloc[0], line_width=2, line_dash="dashdot", line_color="#017EFA")

        # Hover Label
        comment_alert.update_layout(hoverlabel=dict(bgcolor="#c1dfff", font_size=12, font_family="Poppins", align="left"))
        comment_alert.update_traces(hovertemplate="<i><b>%{hovertext}</b></i><br>In: <b>%{x|%b %Y}</b><br>Total Alerts: <b>%{y}</b><extra></extra>")

        if((alert_value is not None) and (alert_value != "all")):
            comment_alert.update_layout(title=f"<b>Alerts on Comments Received - {alert_value} Alerts</b>", title_font_color="#052F5F", title_font=dict(size=17, family="Poppins"))
        else:
            comment_alert.update_layout(title="<b>Alerts on Comments Received</b>", title_font_color="#052F5F", title_font=dict(size=17, family="Poppins"))
        return dcc.Graph(figure=comment_alert, config=plot_config)


# Comment Alert Line Chart Slider
@app.callback(
    [Output("comment_alert_line_chart_slider", "marks"), Output("comment_alert_line_chart_slider", "max"),
     Output("comment_alert_line_chart_slider", "min"), Output("comment_alert_line_chart_slider", "value")],
    [Input("member_dropdown", "value")]
)
def update_line_chart_slider(member_value):
    slider_df = df.copy()
    slider_df = slider_df[(slider_df["alert_comments"].str.lower() != "no") & (slider_df["alert_comments"].str.lower() != "") & (slider_df["alert_comments"].notna())]

    # Filters
    slider_df = member_filter(slider_df, member_value)

    slider_df["commentTime_comments"] = pd.to_datetime(slider_df["commentTime_comments"], format="%Y-%m-%d %H:%M:%S").dt.date
    slider_df = slider_df[slider_df["commentTime_comments"] >= date.today()-timedelta(days=365*2)]

    try:
        min_date = slider_df["commentTime_comments"].min()
        max_date = slider_df["commentTime_comments"].max()
        date_range = range((max_date - min_date).days + 1)
    except:
        min_date = todays_date - timedelta(days=365)
        max_date = todays_date
        date_range = range((max_date - min_date).days + 1)

    maximum_mark = max(date_range)
    minimum_mark = min(date_range)
    date_list = [min_date + timedelta(days=i) for i in date_range]

    global date_dict
    date_dict = {i: d.strftime("%Y-%m-%d") for i, d in enumerate(date_list)}
    marks = {i: {"label": d.strftime("%b %Y"), "style": {"fontFamily": "Poppins", "fontWeight": "bold", "fontSize": 10}} for i, d in enumerate(date_list) if ((d.month in [1, 4, 7, 10]) and (d.day == 1))}
    return marks, maximum_mark, minimum_mark, [minimum_mark, maximum_mark]


# Comment Classification Pie Chart
@app.callback(
    Output("comment_classification_pie_chart", "children"),
    [Input("time_control", "value"), Input("date_range_picker", "value"), Input("member_dropdown", "value"), Input("platform_dropdown", "value"), Input("alert_dropdown", "value")]
)
def update_pie_chart(time_value, date_range_value, member_value, platform_value, alert_value):
    result_comment_df = df.copy()
    result_comment_df = result_comment_df[(result_comment_df["result_comments"].str.lower() != "no") & (result_comment_df["result_comments"].str.lower() != "") & (result_comment_df["result_comments"].notna())]
    result_comment_df = result_comment_df[(result_comment_df["alert_comments"].str.lower() != "no") & (result_comment_df["alert_comments"].str.lower() != "") & (result_comment_df["alert_comments"].notna())]

    # Filters
    result_comment_df = time_filter(result_comment_df, time_value, date_range_value)
    result_comment_df = member_filter(result_comment_df, member_value)
    result_comment_df = platform_filter(result_comment_df, platform_value)
    result_comment_df = alert_filter(result_comment_df, alert_value)

    if(len(result_comment_df) == 0):
        return no_data_graph()
    else:
        result_comment_df["createTime_contents"] = pd.to_datetime(result_comment_df["createTime_contents"], format="%Y-%m-%d %H:%M:%S.%f")
        result_comment_df = result_comment_df.groupby(by=["result_comments"], as_index=False)["id_comments"].nunique()
        result_comment_df.columns = ["classification", "count"]
        result_comment_df.sort_values(by=["count"], ascending=True, inplace=True)

        comment_classification = px.pie(result_comment_df, values="count", names="classification", color="classification", color_discrete_map=comment_classification_colors)
        comment_classification.update_layout(margin=dict(l=25, r=25), plot_bgcolor="white", paper_bgcolor="white")
        comment_classification.update_layout(annotations=[dict(text="<b>"+str(result_comment_df["count"].sum())+"</b>", x=0.5, y=0.55, font=dict(family="Poppins", size=28, color="#052F5F"), showarrow=False),
                                                          dict(text="Total Comments", x=0.5, y=0.45, font=dict(family="Poppins", size=18, color="#052F5F"), showarrow=False)]
                                             )
        comment_classification.update_layout(legend={"orientation": "h", "x": 0.5, "y": -0.1, "xanchor": "center", "font": {"family": "Poppins", "color": "#2a3f5f", "size": 12}})
        comment_classification.update_traces(textinfo="percent", texttemplate="<b>%{percent}</b>", textposition="outside")
        comment_classification.update_traces(outsidetextfont=dict(family="Poppins", size=14, color="black"), insidetextorientation="horizontal")
        comment_classification.update_traces(hole=0.65, marker=dict(line=dict(color="white", width=2.5)))

        # Hover Label
        comment_classification.update_layout(hoverlabel=dict(bgcolor="#c1dfff", font_size=12, font_family="Poppins", align="left"))
        comment_classification.update_traces(hovertemplate="<b><i>%{customdata[0]} Comments</i></b><br>Total Comments: <b>%{value}</b><br>% of Total: <b>%{percent}</b><extra></extra>")

        if(((platform_value is not None) and (platform_value != "all")) and ((alert_value is not None) and (alert_value != "all"))):
            comment_classification.update_layout(title={"text": f"<b>Comment Classification - {platform_value} &<br>{alert_value} Alerts</b>"}, title_font_color="#052F5F", title_font=dict(family="Poppins", size=17))
        elif((platform_value is not None) and (platform_value != "all")):
            comment_classification.update_layout(title={"text": f"<b>Comment Classification - {platform_value}</b>"}, title_font_color="#052F5F", title_font=dict(family="Poppins", size=17))
        elif((alert_value is not None) and (alert_value != "all")):
            comment_classification.update_layout(title={"text": f"<b>Comment Classification - {alert_value} Alerts</b>"}, title_font_color="#052F5F", title_font=dict(family="Poppins", size=17))
        else:
            comment_classification.update_layout(title={"text": "<b>Comment Classification</b>"}, title_font_color="#052F5F", title_font=dict(family="Poppins", size=17))
        return dcc.Graph(figure=comment_classification, config=plot_config)


# Content Classification Sunburst Chart
@app.callback(
    Output("content_classification_sunburst_chart", "figure"),
    Input("time_interval", "n_intervals")
)
def update_sunburst_chart(time_interval):
    risk_content_df = df.copy()
    risk_content_df = risk_content_df[(risk_content_df["result_json_contents"].str.lower() != "no") & (risk_content_df["result_json_contents"].str.lower() != "") & (risk_content_df["result_json_contents"].notna())]
    risk_content_df = risk_content_df[(risk_content_df["alert_contents"].str.lower() != "no") & (risk_content_df["alert_contents"].str.lower() != "") & (risk_content_df["alert_contents"].notna())]

    sunburst_data = {"category": [], "subcategory": [], "value": []}
    for index, row in risk_content_df.iterrows():
        result_json_contents = ast.literal_eval(row["result_json_contents"])
        for category, subcategory_values in result_json_contents.items():
            for subcategory, value in subcategory_values.items():
                sunburst_data["category"].append(category)
                sunburst_data["subcategory"].append(subcategory)
                sunburst_data["value"].append(value)

    risk_content_df = pd.DataFrame(sunburst_data)
    risk_content_df = risk_content_df.groupby(by=["category", "subcategory"], as_index=False)["value"].sum()
    risk_content_df = risk_content_df[risk_content_df["value"] != 0]

    content_classification = px.sunburst(risk_content_df, path=["category", "subcategory"], values="value")
    content_classification.update_traces(marker=dict(line=dict(color="white", width=0.1)), insidetextorientation="horizontal")
    return content_classification


# Content Result Treemap
@app.callback(
    Output("content_result_treemap", "figure"),
    [Input("time_interval", "n_intervals")]
)
def update_content_treemap(time_interval):
    content_json_df = df.copy()

    content_json_df = content_json_df[(content_json_df["result_json_contents"].str.lower() != "no") & (content_json_df["result_json_contents"].str.lower() != "") & (content_json_df["result_json_contents"].notna())]
    treemap_data = {"category": [], "subcategory": [], "value": []}

    for index, row in content_json_df.iterrows():
        result_json_contents = ast.literal_eval(row["result_json_contents"])
        for category, subcategory_values in result_json_contents.items():
            for subcategory, value in subcategory_values.items():
                treemap_data["category"].append(category)
                treemap_data["subcategory"].append(subcategory)
                treemap_data["value"].append(value)

    content_json_df = pd.DataFrame(treemap_data)
    content_json_df = content_json_df[content_json_df["value"] != 0]
    content_json_df = content_json_df.groupby(by=["category", "subcategory"], as_index=False)["value"].count()
    content_json_df.sort_values(by=["category", "value"], ascending=[True, False], inplace=True)

    content_treemap = px.treemap(content_json_df, path=[px.Constant("Content"), "category", "subcategory"], values="value", color="category", color_discrete_map={})
    content_treemap.update_layout(margin=dict(t=20, l=0, r=0, b=0))
    content_treemap.update_layout(uniformtext=dict(minsize=10, mode="hide"))
    content_treemap.update_traces(marker=dict(cornerradius=10), root_color="white")
    return content_treemap


# Comment Result Radar
@app.callback(
    Output("comment_result_radar", "figure"),
    [Input("time_interval", "n_intervals")]
)
def update_radar_chart(time_interval):
    comment_json_df = df.copy()
    comment_json_df = comment_json_df[(comment_json_df["result_json_comments"].str.lower() != "no") & (comment_json_df["result_json_comments"].str.lower() != "") & (comment_json_df["result_json_comments"].notna())]
    comment_json_df = pd.DataFrame(list(comment_json_df["result_json_comments"].apply(ast.literal_eval)))
    comment_json_df = comment_json_df.to_dict(orient="list")
    comment_json_df = {key: np.mean([x for x in values if not np.isnan(x)])*100 for key, values in comment_json_df.items()}

    values = comment_json_df.values()
    categories = comment_json_df.keys()
    text = [str(round(value, 2))+"%" for value in values]

    fig = px.line_polar(r=values, theta=categories, line_close=True, markers=True, text=text)
    fig.update_layout(template=None, polar=dict(bgcolor="rgba(255, 255, 255, 0.2)"))
    fig.update_traces(fill="toself", textposition="top center")
    return fig


# Content Result Bubble Chart
@app.callback(
    Output("content_result_bubble_chart", "figure"),
    [Input("time_interval", "n_intervals")]
)
def update_bubble_chart(time_interval):
    content_df = df.copy()

    content_df = content_df[(content_df["result_json_contents"].str.lower() != "no") & (content_df["result_json_contents"].str.lower() != "") & (content_df["result_json_contents"].notna())]
    content_df = content_df[["result_json_contents", "createTime_contents"]]
    content_df = content_df.drop_duplicates()

    content_result_df = pd.DataFrame(columns=["category", "subcategory", "value", "date"])
    for index, row in content_df.iterrows():
        json_data = json.loads(row["result_json_contents"].replace("'", '"'))
        for category, subcategories in json_data.items():
            for subcategory, value in subcategories.items():
                content_result_df.loc[len(content_result_df.index)] = [category, subcategory, value, str(row["createTime_contents"])]

    fig = go.Figure(data=go.Scatter(x=[1, 2, 3, 4], y=[10, 11, 12, 13], mode="lines"))
    return fig


# Running Main App
if __name__ == "__main__":
    app.run_server(debug=False)
