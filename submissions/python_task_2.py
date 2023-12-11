#install networkx library
!pip install networkx
import pandas as pd
import networkx as nx
import itertools
import datetime

def calculate_distance_matrix(df)->pd.DataFrame():
    """
    Calculate a distance matrix based on the dataframe, df.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Distance matrix
    """
    #defining function within function for optimized calculation
    G, distance_df = nx.Graph(), pd.DataFrame()
    G.add_nodes_from(df.id_start.to_list())
    #helper function
    def create_graph(row):
        G.add_edge(row['id_start'], row['id_end'], weight = row['distance'])
    df.apply(create_graph, axis = 1)
    nodes_list = list(set(df.id_start).union(set(df.id_end)))
    nodes_tuple = list(itertools.product(nodes_list, nodes_list))
    distance_df['id_start'] = [x[0] for x in nodes_tuple]
    distance_df['id_end'] = [x[1] for x in nodes_tuple]
    #helper function
    def shortest_distance(row):
        return nx.shortest_path_length(G, row.id_start, row.id_end, weight="weight")
    distance_df['distance'] = distance_df.apply(shortest_distance, axis = 1)
    distance_df = distance_df.pivot(index = 'id_start', columns = 'id_end', values = 'distance')
    return distance_df


def unroll_distance_matrix(df)->pd.DataFrame():
    """
    Unroll a distance matrix to a DataFrame in the style of the initial dataset.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Unrolled DataFrame containing columns 'id_start', 'id_end', and 'distance'.
    """
    distance_df = df.unstack().reset_index(name = 'distance')
    distance_df = distance_df.loc[distance_df.id_end != distance_df.id_start].reset_index(drop = True)
    return distance_df


def find_ids_within_ten_percentage_threshold(df, reference_id)->pd.DataFrame():
    """
    Find all IDs whose average distance lies within 10% of the average distance of the reference ID.

    Args:
        df (pandas.DataFrame)
        reference_id (int)

    Returns:
        pandas.DataFrame: DataFrame with IDs whose average distance is within the specified percentage threshold
                          of the reference ID's average distance.
    """
    #instructions regarding Q3 are unclear as it states "return a sorted list of values"
    #whereas in function doc string it states return dataframe and another issue is in logic which ambiguous;
    # here we will return df with id_start satisfying the condition whose distance from reference_id
    #satisfies the condition
    threshold_df = df.loc[df.id_end == reference_id]
    avg = threshold_df.distance.mean()
    threshold_df = threshold_df.loc[(avg*0.9 <= threshold_df.distance) & (threshold_df.distance <= avg*1.1)]
    return threshold_df


def calculate_toll_rate(df)->pd.DataFrame():
    """
    Calculate toll rates for each vehicle type based on the unrolled DataFrame.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    #in picture distance column has been dropped whereas in instruction it has not been mentioned
    #here we will not drop the distance column
    distance_df = df.copy() #ensures no change in input dataframe
    distance_df['moto'] = distance_df.apply(lambda x: x.distance*0.8, axis = 1)
    distance_df['car'] = distance_df.apply(lambda x: x.distance*1.2, axis = 1)
    distance_df['rv'] = distance_df.apply(lambda x: x.distance*1.5, axis = 1)
    distance_df['bus'] = distance_df.apply(lambda x: x.distance*2.2, axis = 1)
    distance_df['truck'] = distance_df.apply(lambda x: x.distance*3.6, axis = 1)
    #return distance_df.drop(columns = 'distance')
    return distance_df


def calculate_time_based_toll_rates(df)->pd.DataFrame():
    """
    Calculate time-based toll rates for different time intervals within a day.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame
    """
    #expected output is ambiguous as output in picture and through following procedure will be different
    #in out implementaion we have combined both the outputs or another issue - input is mentioned of
    #output of question 3 but input is more similar to output of question 4; either of these has to be
    #true; another issue output screenshot shows rate rounded off but in instructions
    #rate not mentioned to be rounded to 2 places
    #new changes as input changed to Question 4
    toll_df = df.copy()
    #create temporary dataframe
    new_df = pd.DataFrame(columns = ['startDay', 'start_time', 'endDay', 'end_time', 'discount_factor'])
    days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    time = ['00:00:00', '10:00:00', '18:00:00', '23:59:59']
    count = 0
    for i in days:
        for j in range(len(time)-1):
            dis_factor = 0.8 if j == 0 else (1.2 if j == 1 else 0.8)
            new_df = pd.concat([new_df, pd.DataFrame(data = {'startDay':i, 'start_time':time[j], 
                        'endDay':i,'end_time':time[j+1], 'discount_factor':dis_factor}, index = [count])], ignore_index = True)
            count+=1
    new_df = pd.concat([new_df, pd.DataFrame(data = {'startDay':'Saturday', 'start_time':'00:00:00', 
                    'endDay':'Sunday','end_time':'23:59:59', 'discount_factor':0.7}, index = [0])], ignore_index = True)
    new_df['dummy_key'] = 1
    toll_df['dummy_key'] = 1
    time_toll_df = pd.merge(toll_df, new_df, how = 'inner')
    time_toll_df.moto = time_toll_df.moto*time_toll_df.discount_factor
    time_toll_df.car = time_toll_df.car*time_toll_df.discount_factor
    time_toll_df.rv = time_toll_df.rv*time_toll_df.discount_factor
    time_toll_df.bus = time_toll_df.bus*time_toll_df.discount_factor
    time_toll_df.truck = time_toll_df.truck*time_toll_df.discount_factor
    time_toll_df.drop(columns = ['dummy_key', 'discount_factor'], inplace = True)
    time_toll_df.start_time = time_toll_df.start_time.apply(lambda x: datetime.time(
        hour = int(x.split(':')[0]), minute = int(x.split(':')[1]), second = int(x.split(':')[2])))
    time_toll_df.end_time = time_toll_df.end_time.apply(lambda x: datetime.time(
        hour = int(x.split(':')[0]), minute = int(x.split(':')[1]), second = int(x.split(':')[2])))
    return time_toll_df