import pandas as pd
import json
import time

from pandas.io.json import json_normalize
from db import DBConnector
from config import QUERY_FILENAME, JSON_COLUMNS
from sqlalchemy import text


def breakout(df, json_columns):
    """
    Recieves a dataframe a list of column names that contain json objects and returns a dataframe with the json objects broken out into columns, with their names prepended to the new column names.
    :param df: Pandas dataframe
    :param json_columns: List of column names  in df that contain json objects
    :return: Pandas dataframe
    """

    for col in json_columns:
        # Convert json strings in the column to dictionaries
        df[col] = df[col].apply(lambda x: json.loads(x) if x else None)

        # Normalize json dictionaries and create new columns
        expanded_data = json_normalize(df[col])

        # Rename new columns by prepending the json column name
        expanded_data.columns = [f"{col}.{c}" for c in expanded_data.columns]

        # Merge expanded_data with the original dataframe
        df = pd.concat([df, expanded_data], axis=1)

        # Drop the original json column
        df.drop(col, axis=1, inplace=True)

    return df


def query_database(
    query, flavor="mysql", alias="read", dbconfig_fn="~/.dbconfig.yaml", verbose=True
):
    """
    Queries the database using the query passed in and returns the resulting dataframe
    :param query: Query to run
    :param flavor: Flavor of the database to connect to
    :param alias: Alias of the database to connect to
    :param dbconfig_fn: Path to the dbconfig file
    :param verbose: Whether or not to print time elapsed
    :return: Pandas dataframe
    """

    # Use the database connection to query the database and return a pandas series of json objects
    dbc = DBConnector(flavor=flavor, alias=alias, dbconfig_fn=dbconfig_fn)

    # Time the query
    t1 = time.time()
    if verbose:
        print("Querying database...")

    # Query the database
    df = dbc.query(text(query))

    # Print the time elapsed
    if verbose:
        print("Done! Time elapsed: {} seconds".format(round(time.time() - t1), 2))

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
