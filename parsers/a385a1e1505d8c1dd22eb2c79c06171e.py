```python
import pandas as pd

def parse(df):
    # Normalize column names to lowercase
    df.columns = df.columns.str.lower()
    
    # Select and rename columns
    columns_mapping = {
        'time_delivered': 'time_delivered',
        'creative_id': 'creative_id',
        'viewer_id': 'viewer_id',
        'region': 'region'
    }
    selected_columns = df.loc[:, list(columns_mapping.keys())]
    
    # Drop any completely empty rows
    selected_columns.dropna(how='all', inplace=True)
    
    return selected_columns
```