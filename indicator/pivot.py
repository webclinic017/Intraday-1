from decimal import Decimal


def df_calculate_pivot_points(df_lastperiod):
    TWO_PLACES = Decimal("0.01")
    last_period = {}
    last_period['high'] = df_lastperiod.iloc[0].high
    last_period['low'] = df_lastperiod.iloc[0].low
    last_period['close'] = df_lastperiod.iloc[0].close

    # last_period['high'] = Decimal(float(last_period['high']))
    # last_period['low'] = Decimal(float(last_period['low']))
    # last_period['close'] = Decimal(float(last_period['close']))
    last_period['pivot'] = (last_period['high'] + last_period['low'] + last_period['close']) / 3
    last_period['R1'] = 2 * last_period['pivot'] - last_period['low']
    last_period['S1'] = 2 * last_period['pivot'] - last_period['high']
    last_period['R2'] = last_period['pivot'] + (last_period['high'] - last_period['low'])
    last_period['S2'] = last_period['pivot'] - (last_period['high'] - last_period['low'])
    last_period['R3'] = last_period['pivot'] + 2 * (last_period['high'] - last_period['low'])
    last_period['S3'] = last_period['pivot'] - 2 * (last_period['high'] - last_period['low'])
    last_period['BC'] = (last_period['high'] + last_period['low']) / 2
    last_period['TC'] = (last_period['pivot'] + last_period['BC']) + last_period['pivot']

    return last_period


def calculate_pivot_points(last_period):
    last_period['pivot'] = (last_period['high'] + last_period['low'] + last_period['close']) / 3
    last_period['R1'] = 2 * last_period['pivot'] - last_period['low']
    last_period['S1'] = 2 * last_period['pivot'] - last_period['high']
    last_period['R2'] = last_period['pivot'] + (last_period['high'] - last_period['low'])
    last_period['S2'] = last_period['pivot'] - (last_period['high'] - last_period['low'])
    last_period['R3'] = last_period['pivot'] + 2 * (last_period['high'] - last_period['low'])
    last_period['S3'] = last_period['pivot'] - 2 * (last_period['high'] - last_period['low'])
    last_period['BC'] = (last_period['high'] + last_period['low']) / 2
    last_period['TC'] = (last_period['pivot'] - last_period['BC']) + last_period['pivot']

    last_period['pivot'] = round(last_period['pivot'], 2)
    last_period['R1'] = round(last_period['R1'], 2)
    last_period['S1'] = round(last_period['S1'], 2)
    last_period['R2'] = round(last_period['R2'], 2)
    last_period['S2'] = round(last_period['S2'], 2)
    last_period['R3'] = round(last_period['R3'], 2)
    last_period['S3'] = round(last_period['S3'], 2)
    last_period['BC'] = round(last_period['BC'], 2)
    last_period['TC'] = round(last_period['TC'], 2)

    return last_period
