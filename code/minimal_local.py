from datetime import datetime

import polars as pl
from deltalake import DeltaTable
from polars.io.delta import _convert_pa_schema_to_delta


def execute():
    df = pl.DataFrame(
        {
            "sales_order_id": ["1000", "1001", "1002", "1003"],
            "product": ["bike", "scooter", "car", "motorcycle"],
            "order_date": [
                datetime(2023, 1, 1),
                datetime(2023, 1, 5),
                datetime(2023, 1, 10),
                datetime(2023, 2, 1),
            ],
            "sales_price": [120.25, 2400, 32000, 9000],
            "paid_by_customer": [True, False, False, True],
        }
    )
    print(df)

    df.write_delta("/tmp/sales_orders", mode="append")

    new_data = pl.DataFrame(
        {
            "sales_order_id": ["1002", "1004"],
            "product": ["car", "car"],
            "order_date": [datetime(2023, 1, 10), datetime(2023, 2, 5)],
            "sales_price": [30000.0, 40000.0],
            "paid_by_customer": [True, True],
        }
    )

    dt = DeltaTable("/tmp/sales_orders")
    source = new_data.to_arrow()
    delta_schema = _convert_pa_schema_to_delta(source.schema)
    source = source.cast(delta_schema)

    (
        dt.merge(
            source=source,
            predicate="s.sales_order_id = t.sales_order_id",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


if __name__ == "__main__":
    execute()
