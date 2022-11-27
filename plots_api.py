import plotly.express as px
import plotly.graph_objects as go
from dash import dash_table
from spark_api import get_pandas_by_day_victims, get_pandas_by_month, get_pandas_by_month_victims,get_pandas_by_year,get_pandas_by_day, get_pandas_by_year_victims, get_pandas_filtered, get_pandas_num_crimes_on_crime_type, get_pandas_num_crimes_on_neighborhood, get_pandas_num_victims_on_crime_type, get_pandas_num_victims_on_neighborhood, get_pandas_scatter_plot_matrix, get_pandas_timeline, get_pandas_timeline_victims, get_pandas_victims_num_ranked

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

    if(geo_df.shape[0]==0): 
        return {}

    fig = px.scatter_mapbox(geo_df, lat=geo_df.GEO_LAT, lon=geo_df.GEO_LON, zoom=10, height=300,hover_data=['NEIGHBORHOOD_ID','OFFENSE_CATEGORY_ID'],color='VICTIM_COUNT')
    fig.update_layout(
        mapbox_style="open-street-map")
    

    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    # set graph title
    fig.update_layout(title_text='Map of Crimes in Denver')

    #set height of graph
    fig.update_layout(height=700)
    return fig



def get_scatter_num_crimes_on_day(df):
    df_pd = get_pandas_by_day(df)
    fig = px.scatter(df_pd, x='date', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    # set graph title
    fig.update_layout(title_text='Number of Crimes on Day')
    return fig
def get_scatter_num_crimes_on_month(df):
    df_pd = get_pandas_by_month(df)
    fig = px.scatter(df_pd, x='date', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    # set graph title
    fig.update_layout(title_text='Number of Crimes on Month')
    return fig

def get_scatter_num_crimes_on_year(df):
    df_pd = get_pandas_by_year(df)
    fig = px.scatter(df_pd, x='date', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    # set graph title
    fig.update_layout(title_text='Number of Crimes on Year')
    return fig

def get_bar_num_crimes_on_neighborhood(df):
    df_pandas = get_pandas_num_crimes_on_neighborhood(df)
    fig = px.bar(df_pandas, x='NEIGHBORHOOD_ID', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    # set graph title
    fig.update_layout(title_text='Number of Crimes on Neighborhood')
    return fig

def get_bar_num_crimes_on_crime_type(df):
    df_pandas = get_pandas_num_crimes_on_crime_type(df)
    fig = px.bar(df_pandas, x='OFFENSE_CATEGORY_ID', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    # set graph title
    fig.update_layout(title_text='Number of Crimes on Crime Type')
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

    #set graph title
    fig.update_layout(title_text='Timeline of Crimes in Denver')
    
    return fig



def get_scatter_num_victims_on_day(df):
    df_pd = get_pandas_by_day_victims(df)
    fig = px.scatter(df_pd, x='date', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    # set graph title
    fig.update_layout(title_text='Number of Victims on Day')
    return fig
def get_scatter_num_victims_on_month(df):
    df_pd = get_pandas_by_month_victims(df)
    fig = px.scatter(df_pd, x='date', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    # set graph title
    fig.update_layout(title_text='Number of Victims on Month')
    return fig

def get_scatter_num_victims_on_year(df):
    df_pd = get_pandas_by_year_victims(df)
    fig = px.scatter(df_pd, x='date', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    # set graph title
    fig.update_layout(title_text='Number of Victims on Year')
    return fig

def get_bar_num_victims_on_neighborhood(df):
    df_pandas = get_pandas_num_victims_on_neighborhood(df)
    fig = px.bar(df_pandas, x='NEIGHBORHOOD_ID', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    # set graph title
    fig.update_layout(title_text='Number of Victims on Neighborhood')
    return fig

def get_bar_num_victims_on_crime_type(df):
    df_pandas = get_pandas_num_victims_on_crime_type(df)
    fig = px.bar(df_pandas, x='OFFENSE_CATEGORY_ID', y='count',color='count', template='plotly_dark', color_discrete_sequence=plot_palette)
    # set graph title
    fig.update_layout(title_text='Number of Victims on Crime Type')
    return fig


def get_map_timeline_victims(df):
    df_pandas = get_pandas_timeline_victims(df)

    # animation frame based on date_month, bar plot
    # color based on count_crimes
    fig = px.bar(df_pandas, x="count_crimes", y="NEIGHBORHOOD_ID", color="count_crimes", animation_frame="date_month", hover_data=['count_crimes'],template='plotly_dark', color_discrete_sequence=plot_palette)
    # make the animation slower
    fig.layout.updatemenus[0].buttons[0].args[1]["frame"]["duration"] = 1000
    # set graph dimension
    fig.update_layout(width=700, height=700)   

    #set graph title
    fig.update_layout(title_text='Timeline of Victims in Denver') 
    
    return fig

def get_victims_num_ranked(df):
    df_info = get_pandas_victims_num_ranked(df)
    table_style = {
    'maxHeight': '300px',
    'overflowY': 'scroll',
    'overflowX': 'scroll',
    'border': 'thin lightgrey solid',
    }
    return dash_table.DataTable(df_info.to_dict('records'), [{"name": i, "id": i} for i in df_info.columns],
                                style_data={
                                    'whiteSpace': 'normal',
                                    'height': '30px',
                                    'width': '20px',
                                    'overflowX': 'auto',
                                    'overflowY': 'auto'
                                
                                })

def get_scatter_plot_matrix(df):
    df_pandas = get_pandas_scatter_plot_matrix(df)


    fig = go.Figure(data=go.Heatmap(
                    z=df_pandas,x=df_pandas.columns,y=df_pandas.index)
    )
    fig.update_layout(title_text='Heatmap Crimes-category/Neighborhoods',template='plotly_dark')
    #set graph dimension
    fig.update_layout(width=1000, height=1000)

    return fig