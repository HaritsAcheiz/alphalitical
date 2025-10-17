import pandas as pd
from datetime import datetime, timezone, timedelta

def convert_to_iso(source: str, date_str: str, scraped_at: str) -> str:
    if source == 'kompas':
        try:
            format_to_parse = "Kompas.com- %d/%m/%Y, %H:%M WIB"
            naive_dt = datetime.strptime(date_str, format_to_parse)
            wib_offset = timedelta(hours=7)
            wib_tz = timezone(wib_offset)
            aware_dt = naive_dt.replace(tzinfo=wib_tz)
        except ValueError as ve:
            parsed_dt = date_str.split(' ')
            value = parsed_dt[0]
            unit = parsed_dt[1]
            scraped_datetime = datetime.strptime(scraped_at, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone(timedelta(hours=7)))
            if unit == 'jam':
                delta = timedelta(hours=int(value))
            elif unit == 'menit':
                delta = timedelta(minutes=int(value))
            elif unit == 'detik':
                delta = timedelta(seconds=int(value))
            aware_dt = scraped_datetime - delta

        return aware_dt.isoformat()

if __name__ == "__main__":
    records = pd.read_csv(
        f'data/staging/news_{datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d")}.csv')
    records['published_at'] = records.apply(lambda x: convert_to_iso(x['source'], x['published_at'], x['scraped_at']), axis=1)
    records.to_parquet(
        f'data/transformed/news_{datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d")}.parquet',
        index=False,
        engine='pyarrow',
    )