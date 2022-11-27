import logging
import os
import time
import numpy as np
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import pyspark


import plotly.express as px
from dash.dependencies import Input, Output, State
from pyspark.sql.types import IntegerType,StringType,StructField,StructType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from plots_api import get_bar_num_crimes_on_crime_type, get_bar_num_crimes_on_neighborhood, get_bar_num_victims_on_crime_type, get_bar_num_victims_on_neighborhood, get_map_timeline, get_map_timeline_victims, get_scatter_num_crimes_on_day, get_scatter_num_crimes_on_month, get_scatter_num_crimes_on_year, get_scatter_num_victims_on_day, get_scatter_num_victims_on_month, get_scatter_num_victims_on_year, get_victims_num_ranked, getMap

spark = pyspark.sql.SparkSession.builder.appName("Stars").getOrCreate()
df = spark.read.csv('cleaned_crime.csv', header=True, inferSchema=True)
df = df.withColumn("FIRST_OCCURRENCE_DATE", to_timestamp("FIRST_OCCURRENCE_DATE", "MM/dd/yyyy hh:mm:ss a"))
df = df.withColumn("REPORTED_DATE", to_timestamp("REPORTED_DATE", "MM/dd/yyyy hh:mm:ss a"))




# get the unique values for CRIME_TYPE
crime_types = df.select('OFFENSE_CATEGORY_ID').distinct().toPandas()['OFFENSE_CATEGORY_ID'].tolist()
#get the uniqui values for NEIGHBORHOOD_ID
neighborhoods = df.select('NEIGHBORHOOD_ID').distinct().toPandas()['NEIGHBORHOOD_ID'].tolist()

# get max in VICTIM_COUNT
max_victim_count = df.select(max('VICTIM_COUNT')).collect()[0][0]




# set dash table dimensions






logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
locks = {}

template = 'plotly_dark'
default_layout = {
    'autosize': True,
    'xaxis': {'title': None},
    'yaxis': {'title': None},
    'margin': {'l': 40, 'r': 20, 't': 40, 'b': 10},
    'paper_bgcolor': '#303030',
    'plot_bgcolor': '#303030',
    'hovermode': 'x',
}
plot_config = {
    'modeBarButtonsToRemove': [
        'lasso2d',
        'hoverClosestCartesian',
        'hoverCompareCartesian',
        'toImage',
        'sendDataToCloud',
        'hoverClosestGl2d',
        'hoverClosestPie',
        'toggleHover',
        'resetViews',
        'toggleSpikelines',
        'resetViewMapbox'
    ]
}

external_stylesheets = [
    'https://codepen.io/mikesmith1611/pen/QOKgpG.css',
    'https://codepen.io/chriddyp/pen/bWLwgP.css',
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.8.1/css/all.min.css',
]

def dropdown_options(col):
    return [{'label': name, 'value': name} for name in col]

app = dash.Dash(
    __name__,
    external_stylesheets=external_stylesheets,
    eager_loading=True
)

app.index_string = open('index.html', 'r').read()


def get_graph(class_name, **kwargs):
    return html.Div(
        className=class_name + ' plotz-container',
        children=[
            dcc.Graph(**kwargs),
            html.I(className='fa fa-expand'),
        ],
    )

# create header for dashboard with title and logo
header = html.Div(
    className='header',
    children=[
        html.Div(
            className='header-title',
            children=[
                html.H1('Crime Data'),
                html.H2('Visualizing Crime Data'),
            ],
        ),
        html.Img(
            className='header-logo',
            src=app.get_asset_url('./dash-logo.png'),
        ),
    ],
)

# create dropdown menu for selecting crime type
crime_type = html.Div(
    className='menu',
    children=[
        html.Div(
            className='menu-title',
            children=[
                html.H3('Crime Type'),
            ],
        ),
        dcc.Dropdown(
            id='crime-type',
            options = dropdown_options(crime_types),
            value='All',
        ),
    ]
)

# create dropdown menu for selecting neighborhood
neighborhood = html.Div(
    className='menu',
    children=[
        html.Div(
            className='menu-title',
            children=[
                html.H3('Neighborhood'),
            ],
        ),
        dcc.Dropdown(
            id='neighborhood',
            options = dropdown_options(neighborhoods),
            value='All'
        ),
    ]
)


screen1 = html.Div(
    [
        dcc.Store(id='fig_store'),
        dcc.Graph(id='map-graph1')
    ]
)

crimes_victims = html.Div(
    className='parent',
    children=[
        get_graph('div2',
            figure=get_scatter_num_crimes_on_day(df),
            id='scatter_graph1',
            config=plot_config,
            clear_on_unhover=True
        ),
        get_graph('div2',
            figure=get_bar_num_crimes_on_crime_type(df),
            id='bar-graph2',
            config=plot_config,
            clear_on_unhover=True
        ),
        get_graph('div2',
            figure = get_bar_num_crimes_on_neighborhood(df),
            id='bar-graph1',
            config=plot_config,
            clear_on_unhover=True
        ),
        get_graph('div2',
            figure=get_map_timeline(df),
            id='map_timeline',
            config=plot_config,
            clear_on_unhover=True
        )
    ]
)
information = html.Div(
    className='parent',
    children=[
        get_victims_num_ranked(df)
    ]
)

tmp1 = html.Div(
    className='parent',
    id='tmp-id1',
    children=[],
)

button = html.Div(
    id = 'button-1',
    className='parent',
    children=[
        html.Button('Click here to activate interval', id='button',n_clicks=0)
    ]
)

# define a radio button for selection victims or crimes
radio_victims_crimes = html.Div(
    className='menu',
    children=[
        html.Div(
            className='menu-title',
            children=[
                html.H3('Select Crimes or Victims'),
            ],
        ),
        dcc.RadioItems(
            id='radio-button',
            options=[
                {'label': 'Crimes', 'value': 'Crimes'},
                {'label': 'Victims', 'value': 'Victims'},
            ],
            value='Crimes',
            labelStyle={'display': 'inline-block'}
        ),
    ]
)

#define a radio button for selecting the time period
time_period = html.Div(
    className='menu',
    children=[
        html.Div(
            className='menu-title',
            children=[
                html.H3('Time Period'),
            ],
        ),
        dcc.RadioItems(
            id='time-period',
            className='radio-group',
            options=[
                {'label': 'years', 'value': 'years'},
                {'label': 'months', 'value': 'months'},
                {'label': 'days', 'value': 'days'},
            ],
            value='days',
            labelStyle={'display': 'inline-block'}
        ),
    ],
)

slider_victims = html.Div([
    dcc.Slider(0, 7, 1,
               value=0,
               id='my-slider'
    )
])



# create a div for the number of crimes, victims, and neighborhoods with values returned by a function

interval = html.Div(
    id='interval-div',
    children=[
        dcc.Interval(
            id='interval-component',
            interval=2*1000, # in milliseconds
            n_intervals=0,
            disabled=True
        )
    ]
)

# define app layout with crime_type and screen1
app.layout = html.Div(
    className='container',
    children=[
        interval,
        header,
        slider_victims,
        crime_type,
        neighborhood,
        screen1,
        radio_victims_crimes,
        time_period,
        crimes_victims,
        information,
        tmp1,
        button
    ]
)


# define callback for radio victims or crimes
@app.callback(
    Output('scatter_graph1', 'figure'),
    [Input('radio-button', 'value'),
        Input('time-period', 'value')]
)
def update_scatter_graph1(radio_button, time_period):
    if radio_button == 'Crimes':
        if(time_period=='years'):
            return get_scatter_num_crimes_on_year(df)
        elif(time_period=='months'):
            return get_scatter_num_crimes_on_month(df)
        else:
            return get_scatter_num_crimes_on_day(df)
    else:
        if(time_period=='years'):
            return get_scatter_num_victims_on_year(df)
        elif(time_period=='months'):
            return get_scatter_num_victims_on_month(df)
        else:
            return get_scatter_num_victims_on_day(df)

@app.callback(
    Output('bar-graph1', 'figure'),
    [Input('radio-button', 'value')]
)
def update_bar_graph1(radio_button):
    if radio_button == 'Crimes':
        return get_bar_num_crimes_on_neighborhood(df)
    else:
        return get_bar_num_victims_on_neighborhood(df)

@app.callback(
    Output('bar-graph2', 'figure'),
    [Input('radio-button', 'value')]
)
def update_bar_graph2(radio_button):
    if radio_button == 'Crimes':
        return get_bar_num_crimes_on_crime_type(df)
    else:
        return get_bar_num_victims_on_crime_type(df)

@app.callback(
    Output('map_timeline', 'figure'),
    [Input('radio-button', 'value')]
)
def update_map_timeline(radio_button):
    if radio_button == 'Crimes':
        return get_map_timeline(df)
    else:
        return get_map_timeline_victims(df)


# define callback for dropdown menu that return getMap function
@app.callback(
    Output("fig_store", "data"),
    [
        Input('neighborhood', 'value'),
        Input('crime-type', 'value'),
        Input('my-slider', 'value')
    ])
def update_map(neighborhood,c_type,num_victims):
    return getMap(df,neighborhood,c_type,num_victims)
    
    
app.clientside_callback(
    '''
    function (figure, graph_id) {
        if(figure === undefined) {
            return {'data': [], 'layout': {}};
        }
        var graphDiv = document.getElementById(graph_id);
        var data = figure.data;
        var layout = figure.layout;        
        Plotly.newPlot(graphDiv, data, layout);
    }
    ''',
    Output('map-graph1', 'figure'),
    Input('fig_store', 'data'),
    State('map-graph1', 'id')
)


# define callback for interval
@app.callback(
    Output('interval-component', 'disabled'),
    [Input('button', 'n_clicks'),Input('interval-component', 'n_intervals')]
)
def update_interval(n_clicks,n_intervals):
    if n_clicks==0:
        return dash.no_update
    # get context
    ctx = dash.callback_context
    # get trigger
    if ctx.triggered:
        prop = ctx.triggered[0]['prop_id'].split('.')[0] 
    if prop == 'button':
        return False
    elif prop == 'interval-component' and n_intervals%2==0:
        return True
    else:
        return False
    

@app.callback(
    Output('tmp-id1', 'children'),
    [Input('interval-component', 'n_intervals')]
)
def update_output(n_intervals):
    return str(np.random.randint(100)),True
        




if __name__ == '__main__':
    logger.info('app running')
    port = os.environ.get('PORT', 9000)
    #debug = bool(os.environ.get('PYCHARM_HOSTED', os.environ.get('DEBUG', False)))
    app.run_server(debug=True,
                   host='0.0.0.0',
                   port=port)




