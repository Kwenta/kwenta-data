from decimal import Decimal


def convertDecimals(x):
    try:
        return float(Decimal(x) / Decimal(10**18))
    except:
        return x


def convertBytes(x): return bytearray.fromhex(
    x[2:]).decode().replace('\x00', '')


def clean_df(df, types):
    for col in df.columns:
        type = types[col]
        if type == 'decimal':
            df[col] = df[col].apply(convertDecimals)
        elif type == 'bytes':
            df[col] = df[col].apply(convertBytes)
    return df
