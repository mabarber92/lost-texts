import pandas as pd
import seaborn as sns
from tqdm import tqdm
import re

def prepare_input(leveled_df, self_reuse = None, levels= ["parent_cl_book", "level_1_book", "level_2_book", "level_3_book"], 
                  exclusion_fields = ["No Data", "No Cluster"]):

    
    graphing_data = []
    data_dict_list = leveled_df[levels + ["ms"]].to_dict("records")

    if self_reuse:
        print(self_reuse)
        reuse_author = self_reuse.split(".")[0]
        print(reuse_author)
        

    # Loop through input data so that each level is taken as a hue
    for row in tqdm(data_dict_list):
        for level in levels:            
            data = row[level]
            if data not in exclusion_fields:
                graph_row = {"ms": row["ms"]}
                if self_reuse:
                    author_splits = re.split("\.|'", data)
                    print(author_splits)
                    for item in author_splits:
                        
                        
                        
                        if item == reuse_author:
                            hue = "self_reuse"
                            break
                        else:
                            hue = level
                graph_row["date"] = int(re.findall(r"\d{4}", data)[0])
                graph_row["hue"] = hue
                graphing_data.append(graph_row)
    
    graph_df = pd.DataFrame(graphing_data)
    return graph_df

def scatter_graph(graph_df, image_path):
    ax = sns.scatterplot(data = graph_df, x = "ms", y = "date", hue = "hue")
    fig = ax.get_figure()
    fig.savefig(image_path)

def leveled_df_scatter(csv_path, image_path, self_reuse=None, levels = ["parent_cl_book", "level_1_book", "level_2_book", "level_3_book"]):
    df_data = pd.read_csv(csv_path)
    scatter_graph(prepare_input(df_data, self_reuse, levels), image_path)
    
#Need a graph type that shows area better - so the empty spaces are clearer - something like an area chart/bar

if __name__ == "__main__":
    csv_in = "../data/0845Maqrizi.Mawaciz_leveled_clusters_earliest_source.csv"
    graph_out = "../graphs/0845Maqrizi.Mawaciz_all_levels.png"
    leveled_df_scatter(csv_in, graph_out, self_reuse="0845Maqrizi.Mawaciz", levels = ["parent_cl_book"])


                
