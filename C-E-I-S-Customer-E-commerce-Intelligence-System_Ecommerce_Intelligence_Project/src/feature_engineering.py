from data_integration import r_data_integration
""" SUCCESS ! Nombre de colonnes (39) : Index([
      'order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
       'order_approved_at', 
       'order_delivered_carrier_date',
       'order_delivered_customer_date', 'order_estimated_delivery_date',


       'customer_unique_id', 'customer_zip_code_prefix', 'customer_city',
       'customer_state', 'order_item_id', 'product_id', 'seller_id',

       'shipping_limit_date', 'price', 'freight_value',

       'product_category_name', 'product_name_lenght',
       'product_description_lenght', 'product_photos_qty', 'product_weight_g',
       'product_length_cm', 'product_height_cm', 'product_width_cm',

       'review_id', 'review_score', 'review_comment_title',
       'review_comment_message', 'review_creation_date',
       'review_answer_timestamp', 

       'seller_zip_code_prefix', 'seller_city',
       'seller_state', 

       'payment_sequential', 'payment_type',
       'payment_installments', 'payment_value'],
      dtype='object')   """
import pandas as pd


def create_other_features(df: pd.DataFrame) -> pd.DataFrame:
    df_final = df.copy()
    # Total order value (aggregated from order_items or payments)
    # Logic : la somme total des commandes generer a partir de order_items ou payments
    df_final['total_order_value'] = df_final.groupby('order_id')['price'].transform('sum')

    # Delivery time (order purchase to delivery date)
    # Logic : Delivery time = order_delivered_customer_date - order_purchase_timestamp
    df_final['order_purchase_timestamp'] = pd.to_datetime(df_final['order_purchase_timestamp'])  # i convert to datetime
    df_final['order_delivered_customer_date'] = pd.to_datetime(
        df_final['order_delivered_customer_date'])  # i convert to datetime
    df_final['Delivery_time'] = (
                df_final['order_delivered_customer_date'] - df_final['order_purchase_timestamp']).dt.days

    # Number of items per order
    # logic: need to calculate the number of items, per order. since item are represented by id samw as oder., i will groupe oders  by items
    df_final['number_of_items_per_order'] = df_final.groupby('order_id')['product_id'].transform(
        'nunique')  # "nunique" to have the number of items per order_id

    # Customer purchase frequency
    # logic: its the number of time that an even happens. in this case its how many time the custumer purchase
    df_final['customer_purchase_frequency'] = df_final.groupby('customer_unique_id')['order_id'].transform(
        'nunique')  # confusion here

    # Customer lifetime value (basic approximation)
    # logic:
    df_final['Customer lifetime value'] = df_final.groupby('customer_unique_id')['payment_value'].transform('sum')

    # Average order value per customer
    # logic: the average of oder per customer
    df_final['average_order_value_per_customer'] = df_final['total_order_value'] / df_final['Customer lifetime value']

    return df_final


r_create_other_features = create_other_features(r_data_integration)

print(r_create_other_features)



