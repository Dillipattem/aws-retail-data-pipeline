import boto3
import pandas as pd
import io

s3 = boto3.client('s3')
bucket = 'gluetasks'
input_prefix = 'retail/'
output_prefix = 'prepared/'

# Table schema
files = {
    'customers': ['customer_id', 'customer_name', 'email', 'city', 'state', 'registration_date'],
    'products': ['product_id', 'product_name', 'category', 'price', 'available_stock'],
    'orders': ['order_id', 'customer_id', 'order_date', 'total_amount'],
    'order_items': ['order_item_id', 'order_id', 'product_id', 'quantity', 'item_price'],
    'payments': ['payment_id', 'order_id', 'payment_method', 'payment_date', 'payment_amount']
}

# Process each CSV file
for name, columns in files.items():
    print(f"Processing {name}.csv")
    obj = s3.get_object(Bucket=bucket, Key=f'{input_prefix}{name}.csv')
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), names=columns, header=0)

    # Clean column names
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

    # Transformations
    if name == 'customers':
        df['city'] = df['city'].str.title()
        df['state'] = df['state'].str.upper()
        df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
        df['registration_year'] = df['registration_date'].dt.year

    elif name == 'products':
        df['price'] = df['price'].round(2)
        df['in_stock_flag'] = df['available_stock'].apply(lambda x: 'yes' if x > 0 else 'no')

    elif name == 'orders':
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        df['order_month'] = df['order_date'].dt.strftime('%Y-%m')
        df['order_status'] = df['total_amount'].apply(lambda x: 'Premium' if x > 1000 else 'Standard')

    elif name == 'order_items':
        df['total_price'] = df['quantity'] * df['item_price']

    elif name == 'payments':
        df['payment_date'] = pd.to_datetime(df['payment_date'], errors='coerce')
        df['high_value_flag'] = df['payment_amount'].apply(lambda x: 'yes' if x > 1000 else 'no')

    # Save to Parquet
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)

    out_key = f"{output_prefix}{name}/{name}.parquet"
    print(f"Uploading to: {out_key}")
    s3.put_object(Bucket=bucket, Key=out_key, Body=out_buffer.getvalue())

print("✅ All files processed and written as transformed Parquet.")
