import pandas as pd
import time
from datetime import date
import warnings
warnings.filterwarnings('ignore')

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    data['date'] = pd.Timestamp.now().strftime('%Y-%m-%d')
    data['local_time'] = pd.to_datetime(data['local_time']).dt.strftime('%y-%m-%d %H:%M:%S')
    data['latitude'] = data['latitude'].astype(float)
    data['longitud'] = data['longitud'].astype(float)
    data['temperature'] = data['temperature'].astype(float)
    data['wind_speed'] = data['wind_speed'].astype(float)
    data['date_inserted'] = pd.Timestamp.now().strftime('%y-%m-%d %H:%M:%S')
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
