import seaborn as sns
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib import patches
import seaborn as sns
import re
import json

def graph_source_count(ms_citations_csv, png_out, summary_csv_out, source_name, total_milestones, bin_size = 5, period_map = None, lost_source_list = None):
    """period_map_example = [{"period_name": "Fatimid", "start": 350, "end": 580, colour: "green"}]"""

    sns.set_style("whitegrid")
    # Read in csv
    ms_citations_df = pd.read_csv(ms_citations_csv)

    # Get source counts using groupby
    group_by_ms = ms_citations_df.groupby('ms')['uri'].nunique().reset_index()


    group_by_count = group_by_ms.groupby('uri')['ms'].nunique().reset_index()
    group_by_count = group_by_count.rename(columns = {"uri": "count_of_sources", "ms": "count_of_ms"})
    unsourced_ms = total_milestones - len(ms_citations_df["ms"].drop_duplicates())
    new_row = pd.DataFrame([{"count_of_sources": 0, "count_of_ms": unsourced_ms}])
    group_by_count = pd.concat([group_by_count, new_row])
    group_by_count.to_csv(summary_csv_out)

    # Manually create a histogram-type graph using patches - group together frequencies into 'bins' to create wider, easier to see bars
    if bin_size > 0:
        ms_citations_df = ms_citations_df[["uri","ms"]].drop_duplicates()
        fig, axs = plt.subplots(1, 1)
        fig.set_size_inches(10, 6)
        y_lim = 0
        ms_list = ms_citations_df["ms"].drop_duplicates().to_list()
        ms_type = type(ms_list[0])

        if period_map is not None:
            ms_citations_df["source_date"] = pd.to_numeric(ms_citations_df["uri"].str.split("(\d+)", expand=True)[1])
            

        for i in range(0, total_milestones, bin_size):
            last_ms = i+bin_size
            filtered_ms_list = list(range(i, last_ms))
            
            if ms_type != int:
                filtered_ms_list = [int(i) for i in filtered_ms_list]
            citations_df_filtered = ms_citations_df[ms_citations_df["ms"].isin(filtered_ms_list)].drop_duplicates()
            total_source_count = len(citations_df_filtered)
            if total_source_count > 0:
                if period_map is not None:
                    current_h = 0
                    for period in period_map:                        
                        period_df = citations_df_filtered[citations_df_filtered["source_date"] >= period["start"]]
                        period_df = period_df[period_df["source_date"] < period["end"]]
                        source_count = len(period_df)
                        if source_count > 0:
                            if period["period_name"] not in plt.gca().get_legend_handles_labels()[1]:
                                bin_patch = patches.Rectangle(xy = (i,current_h), width=bin_size -1, height=source_count, color=period["colour"], label=period["period_name"])
                            else:
                                bin_patch = patches.Rectangle(xy = (i,current_h), width=bin_size -1, height=source_count, color=period["colour"])                          
                            axs.add_patch(bin_patch)
                            current_h += source_count
                            if current_h > y_lim:                            
                                y_lim = current_h
                
                elif lost_source_list is not None:
                    source_list = citations_df_filtered["uri"].to_list()
                    
                    lost_count = 0
                    extant_count = 0
                    for source in source_list:
                        if source in lost_source_list:
                            lost_count += 1
                        else:
                            extant_count += 1
                    
                    current_h = 0
                    if extant_count > 0:
                        if "Extant Sources" not in plt.gca().get_legend_handles_labels()[1]:
                            bin_patch = patches.Rectangle(xy = (i,current_h), width=bin_size -1, height=extant_count, color="grey", label="Extant Sources")
                        else:
                             bin_patch = patches.Rectangle(xy = (i,current_h), width=bin_size -1, height=extant_count, color="grey")
                    
                        axs.add_patch(bin_patch)
                        current_h += extant_count
                    if lost_count > 0:
                        if "Lost Sources" not in plt.gca().get_legend_handles_labels()[1]:
                            bin_patch = patches.Rectangle(xy = (i,current_h), width=bin_size -1, height=lost_count, color="lightgrey", label="Lost Sources")
                        else:
                            bin_patch = patches.Rectangle(xy = (i,current_h), width=bin_size -1, height=lost_count, color="lightgrey")
                        axs.add_patch(bin_patch)
                        current_h += lost_count
                    
                    if current_h > y_lim:                            
                        y_lim = current_h                
                


                
                else:

                    bin_patch = patches.Rectangle(xy=(i,0), width = bin_size-1, height= total_source_count, color = "lightgrey")
                    if total_source_count > y_lim:
                        y_lim = total_source_count
                    axs.add_patch(bin_patch)
                    
        
        axs.set_xlim(0, total_milestones)
        axs.set_ylim(0, y_lim + 1)
        axs.set_xlabel("Milestone in the {}".format(source_name))
        axs.set_ylabel("Number of sources per {} milestones".format(bin_size))
        axs.legend()
        fig.savefig(png_out, dpi=300, bbox_inches = "tight")
                    
                    




    
    else:
        plt.bar(group_by_ms["ms"], group_by_ms["uri"])
        plt.show()


if __name__ == "__main__":
    
    period_map = [ 
        {'start': 1, 'end': 357, 'period_name': 'pre-Fatimid',  "colour": "lightgray"}, 
        {'start': 358, 'end': 567, 'period_name': 'Fatimid',  "colour": "darkgray"},
        {'start': 568, 'end': 648, 'period_name': 'Ayyubid',  "colour": "gray"},
        {'start': 649, 'end': 922, 'period_name': 'Mamluk', "colour": "black"},
        {'start': 922, 'end': 1200, 'period_name': 'post-Mamluk',  "colour": "green"}]

    main_text = "../data/0845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown"
    with open(main_text, "r", encoding='utf8') as f:
        text = f.read()
    ms_count = len(re.findall(r"ms\d+", text))
    print("Total ms in {}: {}".format(main_text, ms_count))

    metadata = "F:/Corpus Stats/2023/OpenITI_metadata_2023-1-8.csv"
    meta = pd.read_csv(metadata, encoding='utf-8-sig', sep='\t')
    book_uris = meta["book"].to_list()
    author_uris = [book.split(".")[0] for book in book_uris]

    citation_map = "../citation_resolution/outputs/data/uri_cit_map3.json"
    with open(citation_map, encoding='utf-8-sig') as f:
        cit_dict = json.load(f)

    lost_sources = []
    for item in cit_dict:
        if item in book_uris:
            continue
        elif item in author_uris:
            continue
        else:
            lost_sources.append(item)
    
    print(lost_sources)


    csv = "../outputs_2/citations_with_aligned.csv"
    graph_source_count(csv, "test-patches-sources-by-period.png", "milestones-count-by-sources-found.csv", "Khiṭaṭ", ms_count, period_map = period_map)