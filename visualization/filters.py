import pandas as pd


def filter_algorithm(df: pd.DataFrame, algorithm: str,) -> pd.DataFrame:
    return df[df["algorithm"] == algorithm].copy()


def filter_mode(df: pd.DataFrame, mode: str,) -> pd.DataFrame:
    return df[df["mode"] == mode].copy()


def filter_dataset(df: pd.DataFrame, dataset: str,) -> pd.DataFrame:
    return df[df["dataset"] == dataset].copy()


def filter_data_size(df: pd.DataFrame, data_size: int,) -> pd.DataFrame:
    return df[df["data_size"] == data_size].copy()


def filter_cores(df: pd.DataFrame, cores: int,) -> pd.DataFrame:
    return df[df["cores"] == cores].copy()


def filter_status(df: pd.DataFrame, status: str,) -> pd.DataFrame:
    return df[df["status"] == status].copy()


def filter_parallel(df: pd.DataFrame,) -> pd.DataFrame:
    return filter_mode(df, "Parallel")


def filter_sequential(df: pd.DataFrame,) -> pd.DataFrame:
    return filter_mode(df, "Sequential")


def sort_by_column(df: pd.DataFrame, column: str, ascending: bool = True,) -> pd.DataFrame:
    return df.sort_values(by=column, ascending=ascending,).copy()


def sort_by_data_size(df: pd.DataFrame,) -> pd.DataFrame:
    return sort_by_column(df, "data_size")


def sort_by_cores(df: pd.DataFrame,) -> pd.DataFrame:
    return sort_by_column(df, "cores")