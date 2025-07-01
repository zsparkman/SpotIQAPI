```python
import pandas as pd

def parse(df):
    # Drop completely empty rows
    df = df.dropna(how='all')
    
    # Normalize column names to lowercase
    df.columns = df.columns.str.lower()
    
    # Select only the required columns and rename them
    selected_columns = ['time_delivered', 'creative_id', 'viewer_id', 'region']
    df = df[selected_columns]
    
    return df
```