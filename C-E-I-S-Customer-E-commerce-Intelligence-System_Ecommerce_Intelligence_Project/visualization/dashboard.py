# ════════════════════════════════════════════════════
# STEP 5 — EXECUTIVE SUMMARY DASHBOARD
# ════════════════════════════════════════════════════

from config.imports import *
from config.settings import *
from visualization.eda_vis import *
from data.cleaner import *


print(f"\n📊 EXECUTIVE SUMMARY DASHBOARD")
print(r_c_cleaning.columns)


total_revenue = r_c_cleaning['total_aed'].sum()
total_orders = len(r_c_cleaning)
avg_order =  r_c_cleaning['total_aed'].mean()
return_rate = r_c_cleaning['returned'].mean()*100
avg_rating    = r_c_cleaning["rating"].mean()


fig = plt.figure(figsize=(20, 12))
fig.suptitle(
    "UAE Retail EDA — Executive Summary Dashboard\n"
    "Erman Willian | LogicMojo Batch Mars 2026",
    fontsize=16, fontweight='bold', y=0.98
)

gs = gridspec.GridSpec(3, 4,hspace=0.5, wspace=0.35)

# KPI Cards — row 1

kpis = [
    (f"{total_revenue/1e6:.1f}M AED", "Total revenue"),
    (f"{total_orders:,}", "Total Orders"),
    (f"{avg_order:.0f} AED", "Average Order"),
    (f"{avg_rating:.1f} ⭐", "Average  Rating")
]

for col, (value, label) in enumerate(kpis):
    ax = fig.add_subplot(gs[0,col])
    ax.set_facecolor(PALETTE['primary'])
    ax.axis('off')
    ax.text(0.5, 0.65, value, va='center', fontsize=18, fontweight='bold',transform=ax.transAxes, color='white',)
    ax.text(0.5, 0.30, label, va='center', fontsize=11, fontweight='bold',transform=ax.transAxes, color='white', alpha=0.9,ha='center')


# Revenue by Category — row 2, col 0-1
ax2 = fig.add_subplot(gs[1, :2])
rev_by_cat_sorted = rev_by_cat.sort_values("total_revenu")

ax2.barh(rev_by_cat_sorted.index, rev_by_cat_sorted["total_revenu"],color= sns.color_palette("Blues_d", len(rev_by_cat)), edgecolor = 'white',)
ax2.set_title("Revenue by Category", fontweight='bold')
ax2.set_xlabel("AED")

# Monthly trend — row 2, col 2-3
ax3 = fig.add_subplot(gs[1, 2:])
ax3.plot(gb_month_num.index, gb_month_num["month_num_by_total_aed"],marker='o', linewidth=2.5,color=PALETTE["success"])
ax3.fill_between(gb_month_num.index,gb_month_num["month_num_by_total_aed"],alpha=0.1, color=PALETTE["success"])

ax3.set_xticks(gb_month_num.index)
ax3.set_xticklabels(gb_month_num["mont_name"], rotation=45, fontsize=8)
ax3.set_title("Monthly Revenue Trend", fontweight='bold')


# City + Payment — row 3
##City
ax4 = fig.add_subplot(gs[2, :2])
city_rev = city_stats["revenue_by_city"].sort_values()
ax4.barh(city_rev.index, city_rev.values,color=PALETTE["purple"],edgecolor='white')
ax4.set_title("Revenue by City", fontweight='bold')

##Payment
ax5 = fig.add_subplot(gs[2, 2:])
pay_count = payement_stats["count_order_id"]
ax5.pie(pay_count.values,labels=pay_count.index, autopct='%1.0f%%', colors=sns.color_palette("Set2", 5), wedgeprops= {'edgecolor':'white'}, textprops={'fontsize':9})
ax5.set_title("Payment Methods", fontweight='bold')

plt.savefig(OUTPUT_DIR / "executive_dashboard.png",dpi=150, bbox_inches='tight', facecolor='white')

plt.close()
print(f"  ✅ Executive 7_dashboard saved")

