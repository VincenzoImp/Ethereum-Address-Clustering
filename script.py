import networkx
import pandas 

g = networkx.DiGraph()

node_df = pandas.read_csv('address_eth.csv')
edge_df = pandas.read_csv('tx_eth.csv')

for i, row in node_df.iterrows():
    g.add_node(row['address'])

for i, row in edge_df.iterrows():
    g.add_edge(row['from'], row['to'])


networkx.write_graphml(g, 'graph.graphml')


