# ════════════════════════════════════════════════════
# STEP 4 — EDA + VISUALISATIONS
# ════════════════════════════════════════════════════

from visualization.dashboard import *
from config.settings import *
from data.cleaner import *

# ── Q1 : REVENUS PAR CATÉGORIE ────────────────────
print(f"\n📊 Q1: Revenu by Category")
# rev_by_cat = r_c_cleaning.groupby("category").agg()
print(r_c_cleaning.columns)
# ── Q1 : REVENUS PAR CATÉGORIE ────────────────────
print (f"\n📊 Q1: Revenu by Category")
rev_by_cat = r_c_cleaning.groupby("category").agg(total_revenu =("total_aed","sum"),total_orders = ("order_id","count"),average_orders=("total_aed","mean")).sort_values("total_revenu", ascending=False).round(2)
print(rev_by_cat)

# configurattion
fig, axes = plt.subplots(1, 2, figsize=(16, 6))
fig.suptitle("Q1 : REVENUS PAR CATÉGORY", fontsize=14, fontweight='bold')

# Bar horizontale
colors = sns.color_palette("Blues_d", len(rev_by_cat))

axes[0].barh(rev_by_cat.index, rev_by_cat['total_revenu'], color=colors, edgecolor='red')

# je boucle sur le graphe axes[0]
for i, (idx, row) in enumerate(rev_by_cat.iterrows()):  #
    axes[0].text(row["total_revenu"] + 1000000, i, f"{row['total_revenu']:,.0f} AED",
                 # La fonction axes[0].text(X, Y, "Texte", ...) de Matplotlib sert à placer manuellement un texte sur un graphique à des coordonnées précises (X, Y).
                 va='center',
                 fontsize=9)

    axes[0].set_title("Total revenue by category")
    axes[0].set_xlabel("Revenue (AED)")

# Pie
axes[1].pie(
    rev_by_cat["total_revenu"],
    labels=None,  # Supprime les textes qui chevauchent
    autopct="%1.1f%%",
    startangle=90,
    colors=sns.color_palette("husl", len(rev_by_cat)),
    wedgeprops={"edgecolor": "white"},
    pctdistance=0.75,  # Rapproche légèrement les pourcentages du centre
)

# 2. On ajoute une légende propre à côté
axes[1].legend(
    labels=rev_by_cat.index,
    title="Catégories",
    loc="center left",
    bbox_to_anchor=(1, 0.5),  # Place la légende à l'extérieur droit du cercle
)

axes[1].set_title("Revenue share by category")
plt.tight_layout()
save_fig("q1_revenue_by_category")

# INSIGHT

top_cat = rev_by_cat.index[0]
top_rev = rev_by_cat["total_revenu"].iloc[0]

print("\n 💡 INSIGHT Q1:")

print(f" -> {top_cat} leads with "f"{top_rev:,.0f} AED revenue")
print(f"Top 3 categories = "
      f"{rev_by_cat['total_revenu'].iloc[:3].sum() / rev_by_cat['total_revenu'].sum():.1%}"
      f"of total revenue")



## ── Q2 : ACTIVITÉ PAR VILLE ───────────────────────

print(r_c_cleaning.columns)

print(f"\n📊 Q2: Activity by City")

city_stats = r_c_cleaning.groupby('city').agg(orders_by_city = ("order_id","count"),revenue_by_city=("total_aed","sum"),average_order_by_city = ("total_aed","mean"),returned_by_city = ("returned","count")).sort_values("revenue_by_city", ascending=False).round(2)
print(city_stats)

fig, axes = plt.subplots(1, 3, figsize=(18, 5))
fig.suptitle("Q2 — Activity by City", fontsize=20, fontweight='bold')

city_order = city_stats.index.tolist()

# on s'interresse au "orders_by_city" pour le axes[0]

# orders_by_city
sns.barplot(
    ax=axes[0],
    data=r_c_cleaning,
    x='city',
    y='total_aed',
    order=city_order,  # pour ordeonner  les bar dans le graphique
    estimator=sum,
    palette='viridis',

)
axes[0].set_title("Total revenu by city")
axes[0].set_ylabel("Revenue AED")
axes[0].tick_params(axis='x', rotation=20)

# on s'interresse au "average_order_by_city" pour le axes[1]

sns.barplot(
    ax=axes[1],
    data=r_c_cleaning,
    x='city',
    y='total_aed',
    # order = city_order, # pour ordeonner  les bar dans le graphique
    estimator=sum,
    palette='plasma',

)
axes[1].set_title("Average order value by city")
axes[1].set_ylabel("Revenue AED")
axes[1].tick_params(axis='x', rotation=20)

#  on s'interresse au "Category mix per city" pour le axes[2]

pivot_city = pd.crosstab(
    r_c_cleaning['city'],
    r_c_cleaning["category"],
    normalize='index'
) * 100

pivot_city.plot(
    ax=axes[2],
    kind="bar",
    stacked=True,
    colormap="tab10",
    edgecolor='white',
)
axes[2].set_title("Categoty mix by city (%)")
axes[2].tick_params(axis='x', rotation=15)
axes[2].legend(bbox_to_anchor=(1, 1), fontsize=7)

plt.tight_layout()
save_fig("q_2activity_by_city")

# INSIGHT Q2:
print(f"\n  💡 INSIGHT Q2:")

print(f"\n -> Dubai bdominate with"
     f"{city_stats['revenue_by_city'].iloc[0]/city_stats['revenue_by_city'].sum():.1%}"
     f"of the total revenu")
print(f"-> Abu dhabi Average order: "
     f"{city_stats['average_order_by_city'].iloc[1]:.0f} AED"
     f"(potential premium market)")


# ── Q3 : TENDANCES TEMPORELLES ───────────────────
print(f"\n📊 Q3: Temporal Trends")

print(r_c_cleaning.columns)

gb_month_num = r_c_cleaning.groupby("month_num").agg(month_num_by_order=("order_id", "count"),
                                                     month_num_by_total_aed=("total_aed", "sum"),
                                                     avg_order=("total_aed", "mean")).round(2)

gb_month_num["mont_name"] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
]

print(gb_month_num)

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle("Q3 : TENDANCES TEMPORELLES", fontsize=20, fontweight='bold')

# montly revenue
axes[0, 0].plot(
    gb_month_num.index,
    gb_month_num['month_num_by_total_aed'],
    marker='o',
    linewidth=2.5,
    color=PALETTE["primary"]
)

axes[0, 0].fill_between(
    gb_month_num.index,
    gb_month_num['month_num_by_total_aed'],
    alpha=0.1,
    color=PALETTE['primary']
)

axes[0, 0].set_xticks(
    gb_month_num.index,

)

axes[0, 0].set_xticklabels(
    gb_month_num['mont_name'],
    rotation=45

)

axes[0, 0].set_title("Monthly Revenu (AED)")
axes[0, 0].set_ylabel("revenue")

# Order par mois [0,1]

axes[0, 1].bar(gb_month_num.index, gb_month_num['month_num_by_order'], color=PALETTE["success"], edgecolor='white')

axes[0, 1].set_xticks(gb_month_num.index)

axes[0, 1].set_xticklabels(gb_month_num['mont_name'], rotation=45)

axes[0, 1].set_title("Orders per month")

#  par Quarterly  [1,0]
quarterly = r_c_cleaning.groupby("quarter")['total_aed'].sum().round(2)
axes[1, 0].bar(quarterly.index, quarterly.values, color=PALETTE['purple'], edgecolor='white')
for i, (q, v) in enumerate(quarterly.items()):
    axes[1, 0].text(i, v + 500,
                    f"{v:,.0f}",
                    ha='center',
                    fontsize=9)

axes[1, 0].set_title("Revenue par Quater")
axes[1, 0].set_ylabel("Revenue AED")

# day of week
dow_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
dow_rev   = (r_c_cleaning.groupby("weekday")['total_aed'].mean().reindex(dow_order))

axes[1,1].bar(range(7), dow_rev.values, color=PALETTE['warning'],edgecolor='white')
axes[1,1].set_xticks(range(7))
axes[1,1].set_xticklabels(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"],rotation=45)
axes[1,1].set_title("Average Order Value By day of week")
axes[1,1].set_ylabel("Avg AED")
plt.tight_layout()

save_fig("q3_temporal_trends")

peak_month = gb_month_num['month_num_by_total_aed'].idxmax()

print(f"\n  💡 INSIGHT Q3:")

print(f" -> Peack month: "
     f"{gb_month_num.loc[peak_month,'mont_name']}"
     f"{gb_month_num.loc[peak_month,'month_num_by_total_aed']:,.0f}AED")
print(f"Q4 - is typically stronguest"
     f"(Ramadan/hollidays effects)")



# ── Q4 : PROFIL DES RETOURS ───────────────────────
print(f"\n📊 Q4: Return Profile")  ### je me demande si il ya pas un moyen de savoir avec qui faire les groupby
print(r_c_cleaning.columns)


return_stats = r_c_cleaning.groupby("returned").agg(avg_rating = ("rating","mean"),avg_value = ("total_aed","mean"),count=("order_id","count")).round(2)

print(return_stats)
return_stats.index =["Not Returned","Returned"]
print("\n")
print(return_stats)

# configuration de la figure

fig, axes = plt.subplots(1, 3, figsize=(18, 5))
fig.suptitle("Q4: PROFIL DES RETOURS", fontsize=20, fontweight='bold')

# return rate by category
return_by_cat = (r_c_cleaning.groupby("category")["returned"].mean() * 100).sort_values(ascending=False)
axes[0].barh(
    return_by_cat.index,
    return_by_cat.values,
    color=[PALETTE["secondary"]
           if v > return_by_cat.mean()
           else PALETTE["success"]
           for v in return_by_cat.values]
)
axes[0].axvline(
    x=return_by_cat.mean(),
    color='black',
    linestyle='--',
    alpha=0.7,
    label='Average'
)
axes[0].set_title("Return Rate by category (%)")
axes[0].set_xlabel("Return Rate (%)")
axes[0].legend()

# rating distribution - return vs not
sns.histplot(
    ax=axes[1],
    data=r_c_cleaning,
    x="rating",
    hue="returned",
    palette={0: PALETTE['success'],
             1: PALETTE['secondary']},
    bins=20,
    kde=True,
)
axes[1].set_title("Rating: Returned  Vs Not Returned")
axes[1].legend(["Not returned", "Returned"])

# Raturn rate by city

return_city = (r_c_cleaning.groupby("city")["returned"].sum() * 100).sort_values(ascending=False)
axes[2].bar(
    return_city.index,
    return_city.values,
    color=PALETTE["warning"],
    edgecolor='white',

)
axes[2].set_title("Return rate by city (%)")
axes[2].tick_params(axis='x', rotation=15)
axes[2].set_ylabel("Return Rate (%)")
plt.tight_layout()
save_fig("q4_return_profile")


return_rate = r_c_cleaning["returned"].mean() * 100
print(f"\n  💡 INSIGHT Q4:")
print(f"-> Overall return rate : {return_rate:.1f}%")
print(f"-> Returned items have avg rating: "
      f"{r_c_cleaning[r_c_cleaning['returned'] == 1]['rating'].mean():.1f}")
print(f"-> Not Returned: "
      f"{r_c_cleaning[r_c_cleaning['returned'] == 0]['rating'].mean():.1f}")
print(f"-> Low Rating = Strong predictor of return")


# ── Q5 : MÉTHODES DE PAIEMENT ────────────────────
print(f"\n📊 Q5: Payment Methods")
print(r_c_cleaning.columns)

payement_stats = (r_c_cleaning.groupby("payment_method").agg(count_order_id = ("order_id","count"),revenue_aed = ("total_aed","sum"),avg_val =("total_aed","mean"))).sort_values("count_order_id",ascending=False).round(2)
print(payement_stats)

fig, axes = plt.subplots(1,2,figsize=(14,6))
fig.suptitle("Q5 - Payement methods",fontsize=20,fontweight='bold')

colors_pay = sns.color_palette("Set2",5)

# on lance avec count_order_id
axes[0].pie(
    payement_stats['count_order_id'],
    labels = payement_stats.index,
    autopct = '%1.1f%%',
    colors = colors_pay,
    startangle = 90,
    wedgeprops = {'edgecolor':"white"}
)

axes[0].set_title('Orders by payement method')


# Avg order value
sns.barplot(
    x=payement_stats["avg_val"],
    y=payement_stats.index,
    palette="Set2",
    ax=axes[1]
)
axes[1].set_title("Avg Order Value by Payment Method")
axes[1].set_xlabel("Avg Order Value (AED)")

plt.tight_layout()
save_fig("q5_payment_methods")

top_payement = payement_stats.index[0]
print(top_payement)

print(f"\n  💡 INSIGHT Q5:")
print(f"-> {top_payement} is most popular"
     f"({payement_stats['count_order_id'].iloc[0]/len(r_c_cleaning):.1%})")
print(f"-> Bank transfer has higest avg order: "
     f"{payement_stats.loc[payement_stats['avg_val'].idxmax(),'avg_val']:.0f} AED")

print(f"-> digital paiements (card+apple Pay) = "
     f"{(payement_stats.loc[['Credit Card','Debit Card','Apple Pay'], 'count_order_id'].sum()/len(r_c_cleaning)):.1%}")


# ── Q6 : CORRÉLATION RATING / MONTANT ────────────
print(f"\n📊 Q6: Rating vs Amount Correlation")
print(r_c_cleaning.columns)



corr = r_c_cleaning[['total_aed','rating','quantity','unit_price']].corr()  #  clarrifie ce sur quoi se base ce choix
print(corr.round(3))

fig, axes = plt.subplots(1,2,figsize=(14,6))
fig.suptitle ("Q6 — Rating vs Amount Correlation",fontsize=14, fontweight='bold')

#scatter
sns.scatterplot(
    data = r_c_cleaning.sample(500),
    x = "rating",
    y= "total_aed",
    alpha=0.6,
    s = 30,
    ax = axes[0]
)

axes[0].set_title("Rating vs total order Value")
axes[0].legend(bbox_to_anchor=(1,1), fontsize=7)

# heat map correlation

mask = np.triu(np.ones_like(corr),k=1)
sns.heatmap(
    corr,
    annot = True,
    fmt = ".3f",
    cmap="coolwarm",
    vmin = -1,
    vmax = 1,
    square = True,
    ax = axes[1]
)

axes[1].set_title("Correllation Matrix")
plt.tight_layout()
save_fig("q6_rating_correlation")

rating_amount_corr = corr.loc["rating","total_aed"]
print(f"\n  💡 INSIGHT Q6:")
print(f"Rating - Amount correlation: "
     f"{rating_amount_corr:.3f}"
     f"({'weak' if abs(rating_amount_corr) < 0.3 else 'moderate'})")
print(f"-> High value orders does not neccessary get better ratings ")

# ── Q7 : PRODUITS LES PLUS RENTABLES ─────────────
print(f"\n📊 Q7: Most Profitable Products")
print(r_c_cleaning.columns)

products_stats = r_c_cleaning.groupby("product").agg(
    revenu=("total_aed", "sum"),
    orders=("order_id", "count"),
    avg_rating=("rating", "mean"),
    return_rate=("returned", "mean")
).sort_values("revenu", ascending=False).round(3)

print(products_stats.head(10))

fig, axes = plt.subplots(1, 2, figsize=(16, 7))
fig.suptitle("Q7 — Top Products Analysis",
             fontsize=14, fontweight='bold')

# on s'attaque au top 10 by revenue
top10 = products_stats.head(10)
colors = [PALETTE['success']
          if r < 0.05 else PALETTE['warning']
if r < 0.10 else PALETTE['secondary']
          for r in top10["return_rate"]]

axes[0].barh(
    top10.index,
    top10['revenu'],
    color=colors,
    edgecolor='white',

)
axes[0].set_title("Top 10 products by revenu\n"
                  "(🟢 low returns  🟡 medium  🔴 high)")
axes[0].set_xlabel("Revenue (AED)")

# on s'attaque au Revenu Vs Rating bubble chart

top20 = products_stats.head(20)

scatter = axes[1].scatter(

    top20["avg_rating"],

    top20["revenu"],

    s=top20["orders"] * 3,

    c=top20["return_rate"],

    cmap="RdYlGn_r",

    alpha=0.7,

    edgecolors="white"

)

# annotaion Top 3

for i, (product, row) in enumerate(
        top10.head(3).iterrows()
):
    axes[1].annotate(
        product,
        (row["avg_rating"], row["revenu"]),
        fontsize=7,
        xytext=(5, 5),
        textcoords='offset points',
    )

plt.tight_layout()
save_fig("q7_product_analysis")

best_product = products_stats.index[0]
print(f"\n  💡 INSIGHT Q7:")
print(f"  → Best product: {best_product} "
      f"({products_stats['revenu'].iloc[0]:,.0f} AED)")
print(f"  → High rating + low return = "
      f"star product profile")