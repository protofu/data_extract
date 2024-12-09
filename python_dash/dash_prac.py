from dash import Dash, dcc, html

app = Dash(__name__)

app.layout = html.Div([
    html.H1("My Dash"),
    dcc.Graph(id='my-graph'),
    dcc.Dropdown(
        id='my-dropdown',
        options=[
            {'label' : 'option 1', 'value' : 'opt1'},
            {'label' : 'option 2', 'value' : 'opt2'}
        ],
        value='opt1'
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)
