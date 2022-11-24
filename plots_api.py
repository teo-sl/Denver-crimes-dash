import plotly.express as px
from spark_api import get_pandas_by_month,get_pandas_by_year,get_pandas_by_day, get_pandas_filtered, get_pandas_num_crimes_on_crime_type, get_pandas_num_crimes_on_neighborhood, get_pandas_timeline

x = True
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

plot_palette = [
    '#185d6a',
    '#385e4c',
    '#597043',
    '#7a8339',
    '#9b9530',
    '#bca727',
    '#ddb91e',
    '#ffcc14',
]

empty_plot = px.line(template='plotly_dark', )


def get_default_color(count_col='Confirmed'):
    if count_col == 'Confirmed':
        return '#6195d2'
    if count_col == 'Active':
        return '#2B34B9'
    if count_col == 'Recovered':
        return '#BC472A'


def getMap(df,neighborhood,crime_type,num_victims):
    

    geo_df = get_pandas_filtered(df,neighborhood,crime_type,num_victims)
    print("ciao")
    print(geo_df.head())

    fig = px.scatter_geo(geo_df, lat="GEO_LAT", lon="GEO_LON", hover_data=["OFFENSE_CATEGORY_ID","VICTIM_COUNT"], color="VICTIM_COUNT",scope='usa')
             
    lat_foc = 39.715
    lon_foc = -104.962
    fig.update_layout(
        geo = dict(
            projection_scale=60, #this is kind of like zoom
            center=dict(lat=lat_foc, lon=lon_foc), # this will center on the point
        ))
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    
    return fig

def get_scatter_num_crimes_on_day(df):
    df_pd = get_pandas_by_day(df)
    fig = px.scatter(df_pd, x='date', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    return fig
def get_scatter_num_crimes_on_month(df):
    df_pd = get_pandas_by_month(df)
    fig = px.scatter(df_pd, x='date', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    return fig

def get_scatter_num_crimes_on_year(df):
    df_pd = get_pandas_by_year(df)
    fig = px.scatter(df_pd, x='date', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    return fig

def get_bar_num_crimes_on_neighborhood(df):
    df_pandas = get_pandas_num_crimes_on_neighborhood(df)
    fig = px.bar(df_pandas, x='NEIGHBORHOOD_ID', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    return fig

def get_bar_num_crimes_on_crime_type(df):
    df_pandas = get_pandas_num_crimes_on_crime_type(df)
    fig = px.bar(df_pandas, x='OFFENSE_CATEGORY_ID', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    return fig


def get_map_timeline(df):
    df_pandas = get_pandas_timeline(df)

    # animation frame based on date_month, bar plot
    # color based on count_crimes
    fig = px.bar(df_pandas, x="count_crimes", y="NEIGHBORHOOD_ID", color="count_crimes", animation_frame="date_month", hover_data=['count_crimes'],template='plotly_dark', color_discrete_sequence=plot_palette)
    # make the animation slower
    fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 1000
    # set graph dimension
    fig.update_layout(width=700, height=700)    
    
    return fig

