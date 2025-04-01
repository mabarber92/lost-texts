from citation_resolution.create_evaluation_sheet import loop_through_ms
from utilities.clusterDf import clusterDf
import pandas as pd
from tqdm import tqdm

def fetch_top_reusers_for_uncited(citation_df, cluster_obj, main_text_path, main_book_uri):

    # Get total milestones
    with open(main_text_path, encoding='utf-8-sig') as f:
        text = f.read()
    
    ms_dict = loop_through_ms(text)
    ms_list = pd.DataFrame(ms_dict)["ms"].to_list()
    ms_matched = citation_df["ms"].to_list()

    # Create a dictionary listing main_text milestones for each reuser
    reuser_dict = {}
    non_reuse = []
    for ms in tqdm(ms_list):
        if not ms in ms_matched:
            book_df = cluster_obj.return_cluster_df_for_uri_ms(main_book_uri, int(ms))
            book_df = book_df[book_df["book"] != main_book_uri]
            book_list = book_df["book"].to_list()            
            if len(book_list) == 0:
                non_reuse.append(ms)
            else:
                for book in book_list:
                    if book not in reuser_dict.keys():
                        reuser_dict[book] = [ms]
                    else:
                        if ms not in reuser_dict[book]:
                            reuser_dict[book].append(ms)
    
    # Loop through dictionary to create a df with counts
    df_dicts = []
    for reuser in tqdm(reuser_dict.keys()):
        data = reuser_dict[reuser]
        df_dicts.append({"reuser" : reuser, "ms_count": len(data)})
    
    no_reuse_count = len(non_reuse)
    df_dicts.append({"reuser": "None", "ms_count": no_reuse_count}) 
    
    df_out = pd.DataFrame(df_dicts)
    df_out = df_out.sort_values(by=["ms_count"])
    return df_out


def fetch_source_counts(citation_df):

    source_counts_df = citation_df.groupby("uri")["ms"].nunique().reset_index()
    source_counts_df = source_counts_df.rename(columns={"uri": "source_uri", "ms": "ms count"})
    return source_counts_df

def filter_on_ms_agreement(citation_df, agreement_limit = 2):
    ms_list = citation_df["ms"].drop_duplicates().to_list()
    final_df = pd.DataFrame()
    for ms in ms_list:
        filtered_df = citation_df[citation_df["ms"] == ms]
        sources = filtered_df["uri"].drop_duplicates().to_list()
        for source in sources:
            source_df = filtered_df[filtered_df["uri"] == source]
            origins = source_df["origin"].drop_duplicates().to_list()
            if 'self' in origins or len(origins) >= agreement_limit:
                final_df = pd.concat([final_df, source_df])
    
    print("Number of milestones with sources with agreement of {} is {}".format(agreement_limit, len(final_df["ms"].drop_duplicates())))
    return final_df

def count_lost_sources(citation_df, meta_df):

    lost_authors = []
    lost_books = []
    citation_uris = citation_df["uri"].drop_duplicates().to_list()
    for uri in citation_uris:
        uri_split = uri.split(".")
        
        if len(uri_split) == 1:
            print(uri_split)
            matching_uris = meta_df[meta_df["author_from_uri"] == uri_split[0]]
            if len(matching_uris) == 0:
                lost_authors.append(uri)
        else:
            matching_uris = meta_df[meta_df["book"] == uri]
            if len(matching_uris) == 0:
                lost_books.append(uri)
    
    print(lost_authors)
    print(lost_books)

  



def analyse_cits(citation_csv, cluster_path, meta_path, main_text_path, main_book_uri):
    """Main script for analysis"""

    # cluster_obj = clusterDf(cluster_path, meta_path)
    citation_df = pd.read_csv(citation_csv)
    meta_df = pd.read_csv(meta_path, sep='\t')

    # fetch_source_counts(citation_df).to_csv("outputs/cited_source_counts.csv")
    # fetch_top_reusers_for_uncited(citation_df, cluster_obj, main_text_path, main_book_uri).to_csv("outputs/uncited_ms_reusers.csv")

    # for i in range(2,5):
    #     agreement_df = filter_on_ms_agreement(citation_df, agreement_limit=i)
    #     agreement_df.to_csv("outputs/citations_filtered_by_agreement_of_{}.csv".format(i))

    count_lost_sources(citation_df, meta_df)

if __name__ == "__main__":
    
    main_text = "./data/0845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown"
    minified_clusters = "D:/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500_2.csv"
    meta_path = "E:/Corpus Stats/2023/OpenITI_metadata_2023-1-8.csv"
    main_book_uri = "0845Maqrizi.Mawaciz"
    citation_csv = "outputs/citations_with_aligned.csv"

    analyse_cits(citation_csv, minified_clusters, meta_path, main_text, main_book_uri)

