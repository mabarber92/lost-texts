from create_evaluation_sheet import loop_through_ms, search_ms, merge_results_leveled, parse_list_item, create_evaluation_sheet
import pandas as pd
import json
import re
import os
import numpy as np

def get_pattern_cols(evaluation_df, pattern):
    pattern_cols = []
    for column in evaluation_df.columns:
        if len(re.findall(pattern, column)) == 1 and column not in ['word_start', 'word_end']:
            pattern_cols.append(column)
    
    # Sort the list of columns so that we reassemble the citations in the correct order
    pattern_cols.sort()

    return pattern_cols

def locate_level_ids(id_string, leveled_df_ids):
    """Takes dot-separated id, filters the df and returns the whole df for use"""
    string_ids = id_string.split(".")
    ids = []
    for id in string_ids:
        ids.append(int(id))
    
    ids_df = leveled_df_ids[leveled_df_ids["row_id"].isin(ids)]
    return ids_df

def create_update_uri_citation_map(evaluation_df, leveled_df_ids, results_no_clusters_df, uri_citation_map_json, map_only = False):
    
    # Check if the json already exists - if it does read in the existing data - otherwise initiate empty data
    if os.path.isfile(uri_citation_map_json):
        with open(uri_citation_map_json, encoding='utf-8-sig') as f:
            mapping_data = json.load(f)
    else:
        mapping_data = {} 

    # Get the columns that have the words of the citation in them
    word_cols = get_pattern_cols(evaluation_df, 'word_')
    level_cols = get_pattern_cols(leveled_df_ids, '_book')
    
    
    # Create a new column that is the reassembled citation string - for looking up shared citations later
    evaluation_df["full_string"] = evaluation_df[word_cols].agg(' '.join, axis=1)

    # Filter the found citations - these are rows where cit is 1 and extend is NaN
    found_citations = evaluation_df[evaluation_df["cit"] == 1]
    found_citations = found_citations[found_citations["extend"].isna()]
    found_citations = found_citations.replace({np.nan:None})

    



    # Get the citations that are the full length- we will use these to check against later
    full_length_citations = found_citations[found_citations["word_start"].isna()]
    full_length_citations = full_length_citations[full_length_citations["word_end"].isna()]["full_string"].to_list()
    

    # Loop through the found_citations and use them to repopulate the intermediary dictionary - first pass just populate with new strings
    for citation in found_citations.to_dict("records"):        
        
        # Set ms_cit to false by default - it means that the milestone does n

        # Fetch the citation URI and log whether it is in the corpus
        if citation["book_no"]:            
            uri = citation["book_{}".format((int(citation["book_no"])))]
            in_corpus = True
        elif citation["uri_other"]:
            uri = citation["uri_other"]
            in_corpus = True
        elif citation["new_uri"]:
            uri = citation["new_uri"]
            in_corpus = False
        else:
            print("Data error row: {}".format(citation))
            continue
        
        # Build the citation string based on the word_start and word_end
        word_start = citation["word_start"]
        word_end = citation["word_end"]
        # If word start and end are none - then we create indexes based on the full length of the cols
        if word_start == None or word_end == None:
            word_start = 0
            word_end = len(word_cols)
            full_length = True
        else:
            word_start = word_start - 1
            full_length = False
        cit_words = []
        for word_col in word_cols[int(word_start):int(word_end)]:
            cit_words.append(citation[word_col])
        final_string = " ".join(cit_words)


        # Filter the evaluation_df to find strings that start and end at specified point
        all_citations = evaluation_df.copy()
        for word_col in word_cols[int(word_start):int(word_end)]:
            all_citations = all_citations[all_citations[word_col] == citation[word_col]]
        
        
        

        # To avoid taking cases where the additional length allows us to disambiguate - e.g. where a nisba after the first two words means it's a different name - we use the full-length citations to filter out those cases
        if not full_length:
            all_citations = all_citations[~all_citations["full_string"].isin(full_length_citations)]
            
                  

        # Use the full_string to look up identical strings and collect up the ids - transform those into ms
        
        cit_ids = all_citations["row_id"].to_list()
        
        
        cit_ms = locate_level_ids(".".join(cit_ids), leveled_df_ids)["ms"].drop_duplicates().to_list()
        
        # Fetch shared citations from results_no_clusters and add to ms list
        results_no_clusters_df["full_string"] = results_no_clusters_df[word_cols].agg(' '.join, axis=1)
        
        # Filter the results_no_clusters_df to find strings that start and end at specified point
        all_citations = results_no_clusters_df.copy()
        for word_col in word_cols[int(word_start):int(word_end)]:
            all_citations = all_citations[all_citations[word_col] == citation[word_col]]
        
        # To avoid taking cases where the additional length allows us to disambiguate - e.g. where a nisba after the first two words means it's a different name - we use the full-length citations to filter out those cases
        if not full_length:
            all_citations = all_citations[~all_citations["full_string"].isin(full_length_citations)]
            

        add_ms = all_citations["ms"].to_list()
        cit_ms.extend(add_ms)

        # Use the list of cit_ms to look up the ms and record matches
        ms_reuse = []
        post_ms_reuse = []
        for ms in cit_ms:
            uri_lists = leveled_df_ids[leveled_df_ids["ms"] == ms][level_cols].values.tolist()            
            for uri_list in uri_lists:
                for uri_item in uri_list:
                    if type(uri_item) == str:
                        uri_item = parse_list_item(uri_item)                        
                        if uri in uri_item:
                            ms_reuse.append(ms)
                            
            uri_lists = leveled_df_ids[leveled_df_ids["ms"] == ms + 1][level_cols].values.tolist()            
            for uri_list in uri_lists:
                for uri_item in uri_list:
                    if type(uri_item) == str:
                        uri_item = parse_list_item(uri_item)
                        if uri in uri_item:
                            post_ms_reuse.append(ms+1)
        
        # Remove any duplicates
        ms_reuse = list(dict.fromkeys(ms_reuse))
        post_ms_reuse = list(dict.fromkeys(post_ms_reuse))

            

        
        # Check if a previous loop has added the uri to dict - if so append - else create a new entry in the dict - populate dict with the data
        if uri in mapping_data.keys():
            if final_string not in mapping_data[uri]["citation_strings"]:            
                mapping_data[uri]["citation_strings"].append(final_string)
            mapping_data[uri]["cit_ms"].extend(cit_ms)
            mapping_data[uri]["ms_reuse"].extend(ms_reuse)
            mapping_data[uri]["post_ms_reuse"].extend(post_ms_reuse)
            for key in ["cit_ms", "ms_reuse", "post_ms_reuse"]:
                mapping_data[uri][key] = list(dict.fromkeys(mapping_data[uri][key]))
        else:
            mapping_data[uri] = {"citation_strings" : [final_string],
                                 "cit_ms": cit_ms,
                                 "ms_reuse" : ms_reuse,
                                 "post_ms_reuse": post_ms_reuse,
                                 "in_corpus": in_corpus}


    




        

    
    # At the end write out the data to the place it came from
    json_out = json.dumps(mapping_data, indent=2, ensure_ascii=False)
    with open(uri_citation_map_json, 'w', encoding='utf-8-sig') as f:
        f.write(json_out)


    

    

    # If not map_only produce a list of strings that we need to exclude strings from future matching
    if not map_only:
        non_citations = evaluation_df[evaluation_df["cit"] == 0][word_cols].agg(' '.join, axis=1).to_list()        
        non_citations.extend(found_citations["full_string"].to_list())
        

        return non_citations






def post_evaluation_update(evaluation_csv, uri_citation_map_path, leveled_csv_ids, results_no_clusters_csv, new_evaluation_round = True, text_path = None, new_evaluation_folder=None, main_text_uri = None):        

    # Load in data
    evaluation_df = pd.read_csv(evaluation_csv)
    leveled_df_ids = pd.read_csv(leveled_csv_ids)
    results_no_clusters_df = pd.read_csv(results_no_clusters_csv)

    if not new_evaluation_round:
        # Just update the uri_citation_map_path - in future run other functions to check or analyse data
        create_update_uri_citation_map(evaluation_df, leveled_df_ids, results_no_clusters_df, uri_citation_map_path, map_only = True)

    else:
        # Run the processes that create a new evaluation_csv based on the old one
        with open(text_path, encoding='utf-8-sig') as f:
            text = f.read()

        # Choose the new capture window based on the mode extend
        capture_window = evaluation_df["extend"].dropna().mode().values[0]
        print("Based on previous evaluation df, the new capture window is: {}".format(capture_window))
        print("Proceed with capture window? (y/n)")
        proceed = input()
        while proceed != "y" and proceed != "n":
            print("Invalid - enter y or n")
            proceed = input()
        if proceed == "n":
            print("Enter new capture window")
            capture_window = input()
            while re.match(r"\d+", capture_window) is None:
                print("Enter a valid integer")
                capture_window = input()
            
        capture_window = int(capture_window)


        # Create the exclusion list and update the mapping json
        exclusion_list = create_update_uri_citation_map(evaluation_df, leveled_df_ids, results_no_clusters_df, uri_citation_map_path)
        
        create_evaluation_sheet(leveled_csv_ids, new_evaluation_folder, main_text_path = text_path, main_book_uri = main_text_uri, capture_window=capture_window, exclusion_list = exclusion_list)
        print(exclusion_list)

if __name__ == "__main__":
    evaluation_csv = './outputs/O845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown_evaluation/evaluation_sheet.csv'
    uri_citation_map_path = './outputs/data/uri_cit_map2.json'
    leveled_csv = './outputs/O845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown_supporting_data/leveled_clusters_ids.csv'
    results_no_clusters = './outputs/O845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown_supporting_data/results_no_clusters.csv'
    text_path = '../data/0845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown'
    main_book_uri =
    new_evaluation_folder = './outputs/O845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown2'
    post_evaluation_update(evaluation_csv, uri_citation_map_path, leveled_csv, results_no_clusters, text_path = text_path, new_evaluation_folder=new_evaluation_folder)
