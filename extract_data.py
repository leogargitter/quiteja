import zipfile
import tempfile
import os
import pandas as pd
import argparse
import sqlite3


def unzip_and_read(zip_path: str,
                   data_file: str,
                   types_file: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Unzip a compressed file into a temporary directory,
    read the files contained in the zip and matches it to
    the data and type names given by the user.

    Args:
        zip_path: path to zip file
        data: data file name
        types: types file name

    Returns:
        [DataFrame, DataFrame]: data and type dataframes

    Raises:
        ValueError, FileNotFoundError
    """
    if not zip_path.endswith(".zip"):
        raise ValueError(f"{zip_path} is not a .zip file.")
    if not data_file.endswith(".csv"):
        raise ValueError(f"{data_file} is not a .csv files.")
    if not types_file.endswith(".csv"):
        raise ValueError(f"{types_file} is not a .csv file.")

    with tempfile.TemporaryDirectory() as temp_dir:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)

        files = os.listdir(temp_dir)

        data_df = pd.DataFrame()
        types_df = pd.DataFrame()

        for file in files:
            if file == data_file:
                file_path = os.path.join(temp_dir, file)
                data_df = pd.read_csv(file_path)
            if file == types_file:
                file_path = os.path.join(temp_dir, file)
                types_df = pd.read_csv(file_path)

        if data_df.size == 0:
            raise FileNotFoundError(f"{data_file} not found inside zip.")
        if types_df.size == 0:
            raise FileNotFoundError(f"{types_file} not found inside zip.")

        return data_df, types_df


def filter_and_order_df(data: pd.DataFrame, status: str) -> pd.DataFrame:
    """
    Filter a DataFrame based on "status" column and orders it based on the
    "created_at" column

    Args:
        data: DataFrame to be filtered
        status: status string
    Returns:
        A DataFrame with lines matching with the provided status.
    Raise:
        ValueError
    """
    if "status" not in data.columns:
        raise ValueError("DataFrame don't have a 'status' column")
    if "created_at" not in data.columns:
        raise ValueError("DataFrame don't have a 'created_at' column")

    data = data.loc[data["status"] == status]
    data = data.sort_values(by="created_at")
    return data


def merge_data_types(data: pd.DataFrame, types: pd.DataFrame) -> pd.DataFrame:
    """
    Merges data and type names. Expects that the data DataFrame has a "tipo"
    column and the types DataFrame is composed of an "id" and a "nome" column.

    Args:
        data: data DataFrame
        types: types DataFrame
    Returns:
        Returns a merged DataFrame consisting of the original
        data and its type names.
    Raises:
        ValueError
    """
    if "tipo" not in data.columns:
        raise ValueError("data DataFrame does not contain the column 'tipo'.")
    if "id" not in types.columns:
        raise ValueError("types DataFrame does not contain the column 'id'.")
    if "nome" not in types.columns:
        raise ValueError("types DataFrame does not contain the column 'nome'.")

    merged_df = pd.merge(data, types, left_on="tipo", right_on="id")
    merged_df = merged_df.drop(columns="id")
    merged_df = merged_df.rename(columns={"nome": "nome_tipo"})
    return merged_df


def create_sql_file(zip_path: str,
                    data_file: str,
                    types_file: str,
                    status: str) -> None:
    """
    Creates .sql file based on the zip path, file names and status.

    Args:
        zip_path: path to .zip file
        data_file: name of the data file contained in the .zip
        types_file: name of the types file contained in the .zip
    Returns:
        None
    """
    try:
        data, types = unzip_and_read(zip_path,
                                     data_file,
                                     types_file)
    except (FileNotFoundError, ValueError) as err:
        print(f"Failed reading files: {err}")
        return
    print("Files extracted succesfully!")

    try:
        data = filter_and_order_df(data, status)
        data = merge_data_types(data, types)
    except ValueError as err:
        print(f"Failed to edit data: {err}")
        return
    print("Data edited succesfully!")

    conn = sqlite3.connect("data.db")
    data.to_sql('dados_finais', conn, if_exists='replace', index=False)
    conn.close()

    os.system('sqlite3 data.db .dump > insert-dados.sql')
    print(".sql file created succesfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unzip a file and transform\
                                                  the data into a db.")

    parser.add_argument("zipfile", metavar="zipfile", type=str,
                        help="Path to the zip file.")
    parser.add_argument("-d", "--data", metavar="data", type=str,
                        default="origem-dados.csv",
                        help="Data file, default is 'origem-dados.csv'.")
    parser.add_argument("-t", "--types", metavar="types", type=str,
                        default="tipos.csv",
                        help="Types file, default is 'tipos.csv'.")
    parser.add_argument("-s", "--status", metavar="status", type=str,
                        default="CRITICO",
                        help="Status to be filtered, default is 'CRITICO'.")

    args = parser.parse_args()

    create_sql_file(args.zipfile,
                    args.data,
                    args.types,
                    args.status)
