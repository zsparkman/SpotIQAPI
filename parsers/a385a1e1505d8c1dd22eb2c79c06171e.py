```python
import pandas as pd

def parse(df):
    # Normalize column names to lowercase
    df.columns = df.columns.str.lower()
    
    # Define the required columns
    required_columns = ['time_delivered', 'creative_id', 'viewer_id', 'region']
    
    # Filter the DataFrame to include only the required columns
    df = df[required_columns]
    
    # Drop any completely empty rows
    df = df.dropna(how='all')
    
    return df
```