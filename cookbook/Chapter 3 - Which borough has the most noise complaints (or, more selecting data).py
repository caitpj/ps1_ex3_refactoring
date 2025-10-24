# %%
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt


# Make the graphs a bit prettier, and bigger
plt.style.use("ggplot")
plt.rcParams["figure.figsize"] = (15, 5)

# This is necessary to show lots of columns in pandas 0.12.
# Not necessary in pandas 0.13.
# DELETE pd.set_option("display.width", 5000)
# DELETE pd.set_option("display.max_columns", 60)

# %%
# Let's continue with our NYC 311 service requests example.
# because of mixed types we specify dtype to prevent any errors
# DELETE complaints = pd.read_csv("data/311-service-requests.csv", dtype="unicode")
pl_complaints = pl.read_csv("data/311-service-requests.csv", infer_schema_length=0)

# %%
# TODO: rewrite the above using the polars library (you might have to import it above) and call the data frame pl_complaints

# %%
# 3.1 Selecting only noise complaints
# I'd like to know which borough has the most noise complaints. First, we'll take a look at the data to see what it looks like:
# DELETE complaints[:5]
pl_complaints.head(5)

# %%
# TODO: rewrite the above in polars

# %%
# To get the noise complaints, we need to find the rows where the "Complaint Type" column is "Noise - Street/Sidewalk".
# DELETE noise_complaints = complaints[complaints["Complaint Type"] == "Noise - Street/Sidewalk"]
# DELETE noise_complaints[:3]
noise_complaints = pl_complaints.filter(
    pl.col("Complaint Type") == "Noise - Street/Sidewalk"
)
noise_complaints.head(3)

# %%
# TODO: rewrite the above in polars


# %%
# Combining more than one condition
# DELETE is_noise = complaints["Complaint Type"] == "Noise - Street/Sidewalk"
# DELETE in_brooklyn = complaints["Borough"] == "BROOKLYN"
# DELETE complaints[is_noise & in_brooklyn][:5]
is_noise = pl.col("Complaint Type") == "Noise - Street/Sidewalk"
in_brooklyn = pl.col("Borough") == "BROOKLYN"


# %%
# TODO: rewrite the above using the Polars library. In polars these conditions are called Expressions.
# Check out the Polars documentation for more info.

brooklyn_noise = pl_complaints.filter(is_noise & in_brooklyn)
brooklyn_noise.head(5)

# %%
# If we just wanted a few columns:
# DELETE complaints[is_noise & in_brooklyn][
# DELETE     ["Complaint Type", "Borough", "Created Date", "Descriptor"]
# DELETE ][:10]
brooklyn_noise.select(
    ["Complaint Type", "Borough", "Created Date", "Descriptor"]
).head(10)

# %%
# TODO: rewrite the above using the polars library


# %%
# 3.3 So, which borough has the most noise complaints?
# DELETE is_noise = complaints["Complaint Type"] == "Noise - Street/Sidewalk"
# DELETE noise_complaints = complaints[is_noise]
# DELETE noise_complaints["Borough"].value_counts()
noise_counts = (
    pl_complaints
    .filter(is_noise)
    .group_by("Borough")
    .len()
    .rename({"len": "noise_count"})
)
print(noise_counts)

# %%
# TODO: rewrite the above using the polars library


# %%
# What if we wanted to divide by the total number of complaints?
# DELETE noise_complaint_counts = noise_complaints["Borough"].value_counts()
# DELETE complaint_counts = complaints["Borough"].value_counts()

# DELETE noise_complaint_counts / complaint_counts.astype(float)

total_counts = (
    pl_complaints
    .group_by("Borough")
    .len()
    .rename({"len": "total_count"})
)

# Join noise and total counts
ratios = (
    noise_counts
    .join(total_counts, on="Borough", how="inner")
    .with_columns(
        (pl.col("noise_count") / pl.col("total_count").cast(pl.Float64)).alias("ratio")
    )
)

print(ratios)


# %%
# TODO: rewrite the above using the polars library


# DELETEratios_pd = ratios.to_pandas()   # matplotlib works well with pandas
# DELETEratios_pd.set_index("Borough")["ratio"].plot(kind="bar")

ratios_for_plot = (
    ratios
    .select(["Borough", "ratio"])
    .sort("ratio", descending=True)
)

x = ratios_for_plot.get_column("Borough").to_list()
y = ratios_for_plot.get_column("ratio").to_list()
# %%
# Plot the results
# DELETE (noise_complaint_counts / complaint_counts.astype(float)).plot(kind="bar")

plt.style.use("ggplot")
plt.figure(figsize=(15, 5))
plt.bar(x, y)

plt.title("Noise Complaints by Borough (Normalized)")
plt.xlabel("Borough")
plt.ylabel("Ratio of Noise Complaints to Total Complaints")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %%
# TODO: rewrite the above using the polars library. NB: polars' plotting method is sometimes unstable. You might need to use seaborn or matplotlib for plotting.
