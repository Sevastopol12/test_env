import pandas as pd
import sys
import os
from vnstock import Screener, Trading
from sqlalchemy import create_engine, text
from datetime import date, timedelta


class db_settings:
    connection_string = os.getenv("DATABASE_URI")
    conn = create_engine(url=connection_string)


def load_comparison() -> pd.DataFrame:
    default_params = {
        "exchangeName": "HOSE,HNX",
        "marketCap": (2000, 99999999999),
    }
    comparison: pd.DataFrame = Screener(source="tcbs").stock(
        default_params, limit=1700, lang="en")
    return comparison


def load_price_board(tickers: list[str]) -> pd.DataFrame:
    price_board: pd.DataFrame = Trading(
        source='vci').price_board(symbols_list=tickers)
    price_board.columns = price_board.columns.droplevel(0)

    return price_board


def main() -> None:
    comparison: pd.DataFrame = load_comparison()
    price_board: pd.DataFrame = load_price_board(
        tickers=comparison.ticker.tolist())

    result: pd.DataFrame = pd.merge(
        left=comparison,
        right=price_board,
        left_on='ticker',
        right_on='symbol'
    )

    with db_settings.conn.connect() as connection:
        try:
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS comparison"))
            result.to_sql(
                name="comparison_data",
                con=connection,
                schema="comparison",
                if_exists="replace",
                index=False
            )
            connection.commit()
        except Exception as e:
            connection.rollback()
            print(e)
            sys.exit()


if __name__ == "__main__":
    main()
