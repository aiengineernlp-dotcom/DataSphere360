# ── CONFIGURATION ─────────────────────────────────────
from config.imports import *

sns.set_theme(style="whitegrid", font_scale=1.05)
plt.rcParams.update({
    'figure.facecolor': 'white',
    'axes.spines.top': False,
    'axes.spines.right': False,

})

# Cela cible la racine du projet (le parent du dossier config)
BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR/"output"/"charts"
OUTPUT_DIR.mkdir(parents= True, exist_ok=True)
PALETTE = {
    "primary": "#2980b9",
    "secondary": "#e74c3c",
    "success": "#2ecc71",
    "warning": "#f39c12",
    "purple": "#8e44ad",
    "dark": "#2c3e50",
}


def save_fig(name: str) -> None:
    # Utilisation correcte de pathlib et de la f-string
    plt.savefig(OUTPUT_DIR / f"{name}.png", dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"Save: {name}.png")
