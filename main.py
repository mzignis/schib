import datetime
import json
import pathlib

import pandas as pd
import requests


KEY = 'f4432be357a04c808ff37419'


def get_euro_rate(api_key) -> tuple:

    results = requests.get(f'https://v6.exchangerate-api.com/v6/{api_key}/latest/EUR')
    results = json.loads(results.content)
    timestamp = results["time_last_update_unix"]
    conversion_rates = results['conversion_rates']

    print(f'Last rates update: {datetime.datetime.fromtimestamp(timestamp)}')

    return timestamp, conversion_rates


def convert(budget: float, local_currency: str, conversion_rates: dict) -> float:
    cr = conversion_rates.get(local_currency, None)
    return cr * budget if cr is not None else -1.


def main(input_filepath: pathlib.Path, key: str, separator: str = ';',
         output_filepath: str = None):
    try:
        df = pd.read_csv(input_filepath, sep=separator)
        df['budget_EUR'] = df['budget_EUR'].astype(float)

        ts, euro_rates = get_euro_rate(key)
        ts = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')

        df['budget_LOCAL'] = df.apply(
            lambda x: convert(x['budget_EUR'], x['local_currency'], euro_rates),
            axis=1
        )

        if not output_filepath:
            filename_new = input_filepath.name.replace('input', 'output').replace('.csv', f'_{ts}.csv')
            output_filepath = input_filepath.parent / filename_new

        df.to_csv(output_filepath, sep=separator)
        print(f'Results saved in {output_filepath}')

    except Exception as exception:
        print(f'Cannot calculate local budgets, due: [{type(exception)}] {exception}')



if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', type=str, required=True, help='filepath to input csv')
    parser.add_argument('-o', '--output', type=str, default=None, help='filepath to output csv')
    parser.add_argument('-s', '--separator', type=str, default=';', help='csv separator; default ;')
    parser.add_argument('-k', '--key', type=str, default=KEY, help='key to exchangerate-api.com')

    args = parser.parse_args()
    print(args)
    main(
        input_filepath=pathlib.Path(args.input),
        output_filepath=args.output,
        separator=args.separator,
        key=args.key,
    )