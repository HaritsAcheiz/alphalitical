import pandas as pd
from datetime import datetime, timezone, timedelta

def convert_to_iso(source: str, date_str: str, scraped_at: str) -> str:
    try:
        if source == 'detik':
            format_to_parse = "%Y-%m-%dT%H-%M-%SZ"
            naive_dt = datetime.strptime(date_str, format_to_parse)
        elif source == 'tempo':
            format_to_parse = "%Y-%m-%dT%H:%M:%S%z"
            naive_dt = datetime.strptime(date_str, format_to_parse)
        elif source == 'cnnindonesia':
            format_to_parse = "%Y-%m-%dT%H:%M:%S%z"
            naive_dt = datetime.strptime(date_str, format_to_parse)
        elif source == 'liputan6':
            format_to_parse = "%Y-%m-%dT%H:%M:%S%z"
            naive_dt = datetime.strptime(date_str, format_to_parse)
        elif source == 'kompas':
            format_to_parse = "%Y-%m-%d %H:%M:%S"
            naive_dt = datetime.strptime(date_str, format_to_parse)      
    except ValueError as ve:
        print(f"Error parsing date '{date_str}' from source '{source}': {ve}")
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
        naive_dt = scraped_datetime - delta

    return naive_dt.isoformat()

if __name__ == "__main__":
    records = pd.read_csv(
        f'data/staging/news_{datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d")}.csv')
    records['published_at'] = records.apply(lambda x: convert_to_iso(x['source'], x['published_at'], x['scraped_at']), axis=1)
    records.to_parquet(
        f'data/transformed/news_{datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%d")}.parquet',
        index=False,
        engine='pyarrow',
    )