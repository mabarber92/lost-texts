import seaborn as sns
import pandas as pd
from matplotlib import pyplot as plt

def graph_source_count(ms_citations_csv, png_out, summary_csv_out):

    # Read in csv
    ms_citations_df = pd.read_csv(ms_citations_csv)

    # Get source counts using groupby
    group_by_ms = ms_citations_df.groupby('ms')['uri'].nunique().reset_index()



    group_by_count = group_by_ms.groupby('uri')['ms'].nunique().reset_index()
    group_by_count = group_by_count.rename(columns = {"uri": "count_of_sources", "ms": "count_of_ms"})
    group_by_count.to_csv(summary_csv_out)

        
    plt.bar(group_by_ms["ms"], group_by_ms["uri"])
    plt.show()


if __name__ == "__main__":
    csv = "../outputs_2/citations_with_aligned.csv"
    graph_source_count(csv, "", "milestones-count-by-sources-found.csv")