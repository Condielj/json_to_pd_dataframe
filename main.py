import pandas as pd
from db import DBConnector
from config import QUERY_FILENAME, JSON_COLUMNS
from sqlalchemy import text


def breakout(df, json_columns):
    """
    Recieves a dataframe a list of column names that contain json objects and returns a dataframe with the json objects broken out into columns, with their names prepended to the new column names.
    :param series: Pandas series of json objects
    :param series_name: Name of the series, used to append to the column names
    :param series2: Pandas series of json objects
    :param series2_name: Name of the series2, used to append to the column names
    :return: Pandas dataframe
    """
    # For each column in json_columns, break out the json objects into columns
    for column in json_columns:
        # Create a new dataframe from the json objects in the column
        df_new = pd.json_normalize(df[column])

        # Rename the columns to prepend the column name to the column name
        df_new = df_new.rename(columns=lambda x: column + "_" + x if x != "id" else x)

        # Drop the original column
        df = df.drop(column, axis=1)

        # Join the new dataframe to the original dataframe
        df = df.join(df_new)

    return df


def query_database(query, flavor="mysql", alias="read", dbconfig_fn="~/.dbconfig.yaml"):
    """
    Queries the database using the query passed in and returns the resulting dataframe
    """
    # Use the database connection to query the database and return a pandas series of json objects
    dbc = DBConnector(flavor=flavor, alias=alias, dbconfig_fn=dbconfig_fn)

    # Query the database
    df = dbc.query(text(query))

    return df


def json_to_pandas(query_filename, json_columns):
    """
    Reads in the query from the provided filename, queries the database, and creates a csv of a pandas dataframe with the json objects broken out into columns.
    """
    # Read in the query from the file
    with open(query_filename, "r") as f:
        query = f.read()

    # Query the database
    df = query_database(query)

    # Break out the json objects into columns
    df = breakout(df, json_columns)

    # Create a csv of the dataframe
    df.to_csv("output.csv", index=False)

    return df


if __name__ == "__main__":
    df = json_to_pandas(QUERY_FILENAME, JSON_COLUMNS)
    print(df)
