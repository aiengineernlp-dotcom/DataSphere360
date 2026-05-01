import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from feature_engineering import r_create_other_features


# Exploratory Data Analysis (EDA)


# i will use only numeric column of my r_create_other_features
df_numeric = r_create_other_features.select_dtypes(include='number')
plt.figure(figsize=(12,8))
sns.heatmap(df_numeric.corr(), annot=False, cmap='coolwarm')
plt.title("hidding link in my 46 columns (varaibles)")
plt.show()




# Customer Analysis
# I create here a simple tag for new and repeat custumers
r_create_other_features['customer_type'] = r_create_other_features['customer_purchase_frequency'].apply(lambda x: 'Repeat' if x > 1 else 'New')

#display
sns.countplot(x='customer_type', data=r_create_other_features)
plt.title("custumers repartitions New vs Repeat")
plt.show()



# High-value vs low-value customers
x = r_create_other_features[r_create_other_features['Customer_lifetime_value'] < 900]['Customer_lifetime_value'] # base on the fact that most customer spent let that 1000
#displY
plt.hist(x, bins= 30, color = 'skyblue', edgecolor='black')
plt.title(" Customer lifetime value ")
plt.show()



# Geographic distribution of customers
c_state = r_create_other_features['customer_state'].value_counts()
c_state.plot(kind='barh', color= 'salmon', edgecolor = 'black')
plt.title(" Geographic distribution of customers")
plt.show()





r_create_other_features['customer_state']
x = r_create_other_features['customer_state'].value_counts()
x.plot(kind='bar',figsize=(12,6))
plt.show()






#Revenue and Order Analysis
##Monthly revenue trends

r_create_other_features['order_purchase_timestamp'] = pd.to_datetime(r_create_other_features['order_purchase_timestamp']) # I convert to to_datetime order_purchase_timestamp

r_create_other_features['monthly_year'] = r_create_other_features['order_purchase_timestamp'].dt.to_period('M') # From to_datetime, I create a month_year variable

evolution_ca = r_create_other_features.groupby('monthly_year')['payment_value'].sum() # I grtoupe the monthyly and the payement vaulue sum()

evolution_ca.plot(kind='line',marker='o', figsize=(12,5), color='green')

plt.show()



# Order volume trends

x = r_create_other_features.groupby('monthly_year')['order_id'].nunique()
x.plot(kind ='line', figsize=(12,5),color='green',marker ='s')
plt.grid=True
plt.show()



#Review and Satisfaction Analysis
##Distribution of review scores
compte = r_create_other_features['review_score'].value_counts()
compte.plot(kind='barh')
plt.title("Review distribution")
plt.show()





##Relationship between delivery time and ratings

# y = r_create_other_features['Delivery_time']
# x = r_create_other_features['review_score']
# data = r_create_other_features

# sns.boxplot(x='review_score',  y='Delivery_time', data=df)
# plt.show()



# I chose tyhis because with the code that up i have negatives values on Delivery time
df_graph = r_create_other_features[
    (r_create_other_features['Delivery_time'] >= 0) &
    (r_create_other_features['Delivery_time'] <= 60)
]
y = r_create_other_features['Delivery_time']
x = r_create_other_features['review_score']

sns.boxplot(x='review_score', y='Delivery_time', data=df_graph)
plt.show()




#Identification of dissatisfaction patterns
# i look  those who has the bad reviews
bad_reviews = r_create_other_features[r_create_other_features['review_score']<=2]

# i collect look wich categories are more present in the bad reviews
pattern_category = bad_reviews['product_category_name'].value_counts().head(20)

pattern_category.plot(kind= 'barh')
plt.title('Identification of dissatisfaction patterns top (20)')

plt.show()



#Product Analysis
##Top-selling product categories
revenue_by_cat= r_create_other_features.groupby('product_category_name')['payment_value'].sum().sort_values(ascending=False).head(10)

revenue_by_cat.plot(kind='barh')
plt.title('Top-selling product categories')

plt.show()





#Product demand distribution
product_demand_distribution = r_create_other_features['product_category_name'].value_counts()

plt.figure(figsize=(10, 6))
sns.histplot(product_demand_distribution, bins=30,kde=True,color='purple')

plt.title("Product demand distribution")

plt.show()





#Top-performing sellers
top_perforning_seller = r_create_other_features['seller_id'].value_counts().head(10)
plt.figure(figsize=(10, 6))
top_perforning_seller.plot(kind='barh')


plt.title('Top-performing sellers')




#Seller distribution
seller_distribution = r_create_other_features['seller_state'].value_counts().head(8)
plt.figure(figsize=(12,8))

plt.title('seller_distribution')
seller_distribution.plot(kind='barh')



#Seller contribution to revenue
seller_contribution_to_revenue = r_create_other_features.groupby('seller_id')['payment_value'].sum().sort_values(ascending=False).head(10)
seller_contribution_to_revenue.plot(kind='barh')




#Review and Satisfaction Analysis (Version -2-)
bad_reviewss = r_create_other_features[r_create_other_features['review_score']==1]

product_frequent_with_bad_reviews = bad_reviewss['product_category_name'].value_counts().head(10)
product_frequent_with_bad_reviews.plot(kind='barh')


