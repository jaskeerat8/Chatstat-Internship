# Importing Libraries
import base64
from io import BytesIO
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.patches import Patch

# Custom Font
label_font_path = "assets/fonts/Poppins-SemiBold.ttf"
legend_font_path = "assets/fonts/Poppins-Regular.ttf"
label_prop = fm.FontProperties(fname=label_font_path)
legend_prop = fm.FontProperties(fname=legend_font_path)
legend_prop.set_size(11)

# Colors
content_classification_colors = {"Mental & Emotional Health": "#FFD334", "Other Toxic Content": "#2D96FF", "Violence & Threats": "#FF5100", "Cyberbullying": "#25D366", "Self Harm & Death": "#f77d07", "Sexual & Inappropriate Content": "#a020f0"}

# Matplotlib Image Code
def radial_chart(result_contents_df):
    try:
        categories = result_contents_df["classification"]
        counts = result_contents_df["count"]
        radial = np.radians(result_contents_df["radial"])
        total_radial = np.radians(result_contents_df["total_radial"])
        labels = result_contents_df["classification"]
        colors = [content_classification_colors[category] for category in categories]

        plt.figure().set_figheight(5.5)
        ax = plt.subplot(projection="polar")
        total_radial_bars = ax.barh(categories, total_radial, color="#d8dce2", height=0.8)
        radial_bars = ax.barh(categories, radial, color=colors, edgecolor="black", linewidth=1.5, height=0.77)
        for category, count in zip(categories, counts):
            ax.text(0, category, category + "  (" + str(count) + ") ", color="black", ha="right", va="center",
                    fontsize=12, fontproperties=label_prop)

        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)
        ax.set_xticklabels(["", "", "", "", "", "", f"Total {counts.sum()}       "], fontproperties=label_prop, fontsize=12, color="black")
        ax.set_yticklabels([])
        ax.set_frame_on(False)
        ax.grid(False)

        legend_elements = [Patch(color=color, label=label) for color, label in zip(colors, labels)]
        legend = plt.legend(handles=legend_elements, loc="lower center", bbox_to_anchor=(0.5, -0.23), ncol=2, frameon=False, labelcolor="#052F5F")
        for text in legend.get_texts():
            text.set_fontproperties(legend_prop)
    except Exception as e:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "Issue in Producing Visualisation", ha="center", va="center", fontsize=16, color="red", fontproperties=legend_prop)
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_xticklabels([])
        ax.set_yticklabels([])

    image_buffer = BytesIO()
    plt.tight_layout()
    plt.savefig(image_buffer, format="png", dpi=300)
    plt.close()
    image_data = base64.b64encode(image_buffer.getvalue()).decode("utf8")
    image_buffer.close()
    matplotlib_image = f"data:image/image/png;base64,{image_data}"

    return matplotlib_image

if __name__ == "__main__":
    print("File For Develolping Radial Bar Chart")
