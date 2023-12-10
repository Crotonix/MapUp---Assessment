import pandas as pd
import numpy as np


def generate_car_matrix(df)->pd.DataFrame:
    """
    Creates a DataFrame  for id combinations.

    Args:
        df (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Matrix generated with 'car' values, 
                          where 'id_1' and 'id_2' are used as indices and columns respectively.
    """
    df = df.pivot(index = "id_1", columns = "id_2", values = "car")
    np.fill_diagonal(df.values, 0)
    return df


def get_type_count(df)->dict:
    """
    Categorizes 'car' values into types and returns a dictionary of counts.

    Args:
        df (pandas.DataFrame)

    Returns:
        dict: A dictionary with car types as keys and their counts as values.
    """
    df_car_type = df.copy(deep = False)
    df_car_type['car_type'] = df_car_type.apply(bin_func, axis = 1)
    car_type_count = df_car_type.sort_values(by = 'car_type').car_type.value_counts(sort = False)
    return car_type_count.to_dict()


def get_bus_indexes(df)->list:
    """
    Returns the indexes where the 'bus' values are greater than twice the mean.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of indexes where 'bus' values exceed twice the mean.
    """
    #list of indexes -> indexes/rows in dataframe where condition satisfied have been implemented
    index_list = df.loc[df.bus > 2*df.bus.mean()].index.to_list()
    return sorted(index_list)


def filter_routes(df)->list:
    """
    Filters and returns routes with average 'truck' values greater than 7.

    Args:
        df (pandas.DataFrame)

    Returns:
        list: List of route names with average 'truck' values greater than 7.
    """
    df_filter_routes = df.groupby('route')['truck'].apply(list).reset_index()
    df_filter_routes['avg'] = df_filter_routes.truck.apply(lambda x: sum(x)/len(x))
    filtered_routes = df_filter_routes.loc[df_filter_routes.avg > 7].route.to_list()
    return sorted(filtered_routes)


def multiply_matrix(matrix)->pd.DataFrame:
    """
    Multiplies matrix values with custom conditions.

    Args:
        matrix (pandas.DataFrame)

    Returns:
        pandas.DataFrame: Modified matrix with values multiplied based on custom conditions.
    """
    return matrix.apply(modify_mat)


def time_check(df)->pd.Series:
    """
    Use shared dataset-2 to verify the completeness of the data by checking whether the timestamps for each unique (`id`, `id_2`) pair cover a full 24-hour and 7 days period

    Args:
        df (pandas.DataFrame)

    Returns:
        pd.Series: return a boolean series
    """
    df = df.copy()
    day_encoding = {'monday' : 1, 'tuesday':2, 'wednesday':3, 'thursday':4,'friday':5,
                    'saturday':6, 'sunday':7}
    df.startDay = df.startDay.str.lower().map(day_encoding)
    df.endDay = df.endDay.str.lower().map(day_encoding)
    df['startNum'] = df.startTime.apply(time_to_num)
    df['endNum'] = df.endTime.apply(time_to_num)
    df.startNum = df.startNum + (df.startDay - 1)*86400
    df.endNum = df.endNum + (df.endDay - 1)*86400
    df['range'] = df[['startNum', 'endNum']].apply(tuple, axis = 1)
    df = df.groupby(['id', 'id_2'])['range'].apply(set).reset_index()
    df['timestamp_bool'] = df.range.apply(lambda x: time_check_bool(sorted(x)))
    df['new_index'] = df[['id', 'id_2']].apply(tuple, axis = 1)
    return df.set_index('new_index').timestamp_bool

#helper functions
def bin_func(row):
    if row.car <= 15: return 'low'
    if 15 < row.car <= 25: return 'medium'
    if row.car > 25: return 'high'

def modify_mat(row):
    mult_logic = lambda val: round(val*0.75, 1) if val > 20 else round(val*1.25, 1)
    return list(map(mult_logic, row))

def time_to_num(row):
    hh, mm, ss = map(int, row.split(':'))
    return ss + 60*(mm + 60*hh)

def time_check_bool(check_list):
    time_min, time_max = check_list[0][0], check_list[0][1]
    for x, y in check_list[1:]:
        if time_min <= x and x <= time_max:
            time_max = max(time_max, y)
        elif(time_max + 1 == x):
            #checks if continuous when 11:59:59 and 00:00:00 switch, assumes continuous
            #as both inclusive for the pair or we can change raw data by replacing
            #11:59:59 with 00:00:00; then remove this condition
            time_max = max(time_max, y)
        else:
            return False
    if time_min != 0 or time_max != (60*60*24*7-1):
        return False
    return True