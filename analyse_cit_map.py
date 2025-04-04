# Global Variables
multiprocess = True
pool_size = 16

import json
import pandas as pd
from citation_resolution.create_evaluation_sheet import loop_through_ms
from utilities.clusterDf import clusterDf
from tqdm import tqdm
import re
import os
from openiti.helper.ara import normalize_ara_heavy 
from openiti.helper.funcs import text_cleaner
if multiprocess:
    from multiprocessing import Pool

def identify_continuous_corpus_df(corpus_df, cluster_obj):
    """Take a corpus df loop through each text_uri within it and pass it to identify_continuous_cited_ms to use the reuse
    to infer extended citations"""

    text_uri_list = corpus_df["text_uri"].drop_duplicates().to_list()
    df_out = pd.DataFrame()
    
    for text_uri in text_uri_list:
        print(text_uri)
        uri_df = corpus_df[corpus_df["text_uri"] == text_uri]
        continuous_df = identify_continuous_cited_ms(uri_df, cluster_obj, text_uri, input_type='corpus_df')
        continuous_df["text_uri"] = text_uri
        df_out = pd.concat([df_out, continuous_df])
        df_out.to_csv('outputs_2/continuous_corpus_citations.csv')
    
    return df_out


def identify_continuous_cited_ms(cit_map, cluster_obj, main_book_uri, all_cits=True, input_type = 'cit_map'):
    """Using a cit_map of verified citations, lookup the milestones listed with the citation and a verified extant text, log it as a
    ms. Check if the following ms matches with the book (either as citation or reuse). If not the book, check for author match on same
    principles. When storing continuous reuse, use the identification of a book as precedent - to try and disambiguate citations.
    Continue for each proceeding ms until a match is not found. Outputs df with uri column and ms column 
    
    Takes 2 input types - cit_map or corpus_df - if latter, a slightly different process followed"""
    
    print("Verifying citations against the map and reuse data...")
    
    out_dict_list = []
    # Loop through the keys of the cit_map
    if input_type == 'cit_map':
        uris = list(cit_map.keys())
    elif input_type == 'corpus_df':
        uris = cit_map["uri"].drop_duplicates().to_list()
        int_cit_map = {}
        for uri in uris:
            int_cit_map[uri] = {"cit_ms": cit_map[cit_map["uri"] == uri]["ms"].drop_duplicates().to_list(), "in_corpus": None}
        cit_map = int_cit_map.copy()
            
    else:
        print("Invalid input_type - you specified {}".format(input_type))
        exit()
    for uri in tqdm(uris):
        data = cit_map[uri]
        cit_mss = data["cit_ms"]
        cit_mss.sort()
        resolved_ms = []
        # If the URI is the same as the text - do not try to extend as text reuse evidence is not helpful - just pass it directly into the output
        if uri == main_book_uri:            
            for cit_ms in cit_mss:
                out_dict_list.append({"uri": uri, "ms": cit_ms})                            
                
        # Only treat citations that reference texts or authors in the corpus
        elif data["in_corpus"] or all_cits == True:
            
            # Go through each cited ms and perform series of checks
            for cit_ms in cit_mss:
                # If one of the previous processes has resolved the ms then skip
                if cit_ms not in resolved_ms:
                    continued_quote = True
                    current_ms = cit_ms
                    
                    # If we keep finding ms that match the criteria then continued_quote is true - as soon as we find an ms without a match we break the while loop
                    while continued_quote == True:                                              
                        book_list = cluster_obj.return_cluster_df_for_uri_ms(main_book_uri, current_ms)["book"].to_list()
                                               
                        
                        # If the URI is just an author URI (when split on a . it produces a list of 1) - then create a list for checking a matching author URI - otherwise initiate that list empty
                        if len(uri.split(".")) == 1:
                            
                            author_list = [i.split(".")[0] for i in book_list]
                        else:
                            author_list = []
                        
                        # If the uri is in the author list then find relevant books to append
                        if uri in author_list:
                            # Find the book(s) that match and append the full book URI - this resolves the citation to a book
                            for book in book_list:
                                if book.split(".")[0] == uri:
                                    out_dict_list.append({"uri": book, "ms": current_ms})       
                            resolved_ms.append(current_ms)
                            current_ms = current_ms + 1                                
                                
                        # Otherwise if the book is in the reuse data - easy resolution - just take this URI
                        elif uri in book_list:                          
                            out_dict_list.append({"uri": uri, "ms": current_ms})                            
                            resolved_ms.append(current_ms)
                            current_ms = current_ms + 1                         
                    
                        # If neither criteria is met - see if the ms has a verified citation already                                                    
                        elif current_ms in cit_mss:
                            out_dict_list.append({"uri": uri, "ms": current_ms})                            
                            resolved_ms.append(current_ms)
                            current_ms = current_ms + 1
                            
                        # Non of the criteria have been met - so the ms does not have a match - end the while loop - move on to the next cited ms
                        else:                            
                            continued_quote = False
                        
    out_df = pd.DataFrame(out_dict_list)
    out_df = out_df.drop_duplicates()
    return out_df

def find_unresolved_ms(full_ms_df, ms_matched, data_type = 'ms_df'):
    full_ms_list = full_ms_df["ms"].astype('int32').to_list()
    if data_type == 'cit_map':
        ms_matched_list = []
        for uri in ms_matched.keys():
            data=ms_matched[uri]["cit_ms"]
            for ms in data:
                if ms not in ms_matched_list:
                    ms_matched_list.append(ms)
    else:    
        ms_matched_list = ms_matched["ms"].to_list()
    
    unmatched = []
    for ms in full_ms_list:
        if ms not in ms_matched_list:
            unmatched.append(ms)
    print("Of {} total ms {} are not matched".format(len(full_ms_list), len(unmatched)))
    return unmatched

def search_text_for_cits(text, cit_map, arg2=None, arg3=None):
    """Use a citation map to lookup the citation_strings within a text"""
    # Clean and normalise the text prior to search
    text = text_cleaner(text)
    text = normalize_ara_heavy(text)

    # Run the search
    results = []
    for term in cit_map:
        
        # Use finditer to output a list of dicts [{"citation_uri": uri, "start": start, "end": stop}]
        # Will require finding and fixing a column rename for 'results' in the code somewhere!
        term_result = re.finditer(term["regex"], text)
        for result in term_result:
            result_dict = {"uri": term["citation_uri"], "start": result.start(), "end": result.end()}
            results.append(result_dict)

        # if len(re.findall(term["regex"], text)) > 0:
        #     results.append(term["citation_uri"])
    return results

def normalize_cit_dict(cit_dict):
    """Normalise the citation_strings in the cit_dict so that we match normalised against normalised"""
    for uri in cit_dict.keys():
        strings_in = cit_dict[uri]["citation_strings"]
        cit_dict[uri]["citation_strings"] = [normalize_ara_heavy(string_in) for string_in in strings_in]
    return cit_dict
        


def check_uri_extension(uri_path):
    extensions = ['inProgress', 'completed', 'mARkdown']
    path_found = False
    if os.path.isfile(uri_path):
        path_found = True
        return uri_path
    else:
        split_path = uri_path.split(".")
        if split_path[-1] in extensions:
            pre_extension = ".".join(split_path[:-1])
            if os.path.isfile(pre_extension):
                path_found = True
                return pre_extension
        else:
            pre_extension = uri_path
        for extension in extensions:
            potential_path = pre_extension + "." + extension
            if os.path.isfile(potential_path):
                path_found = True
                return potential_path
    if not path_found:
        return None

def text_path_to_results(text_path, uri_cit_list):        
    print(text_path)
    verified_path = check_uri_extension(text_path["full_path"])
    if verified_path is None:
        print("No related path found")
    else:
        with open(verified_path, encoding='utf-8-sig') as f:
            text = f.read()        
        new_results = loop_through_ms(text, search_text_for_cits, uri_cit_list, separate_lists=False)
        print(len(new_results))            
        
        # If using finditer - refactor so that we parse out the different parts of the results dict into columns
        # In finditer approach return a list of dicts, so:
        results_df = pd.DataFrame()
        for ms in new_results:
            new_results_df = pd.DataFrame(ms["result"])
            new_results_df["ms"] = int(ms["ms"])
            new_results_df["text_uri"] = text_path["book"]
            results_df = pd.concat([results_df, new_results_df])
        return(results_df)


def query_cit_map_corpus(main_book_uri, cit_map, cluster_obj, corpus_base_path, meta_path):
    """A function that queries books aligned with a main_text with the verified citations found in that main text
    Can be used later to lookup clusters and provide potential source resolutions"""

    # Use the clusters to fetch a list of books - get the paths
    aligned_books = cluster_obj.return_cluster_df_for_uri_ms(main_book_uri)["book"].drop_duplicates().to_list()
    meta_df = pd.read_csv(meta_path, sep="\t")
    meta_df = meta_df[meta_df["status"] == "pri"]    
    meta_filtered = meta_df[meta_df["book"].isin(aligned_books)]
    meta_filtered["full_path"] = corpus_base_path + meta_filtered["local_path"].str.split("..", expand=True, regex=False)[1]
    text_paths = meta_filtered[["book", "full_path"]].to_dict("records")
    

    # Use the cit map to build a set of regex search terms in a list of dicts
    uri_cit_list = []
    for uri in cit_map.keys():
        # Fetch citation strings
        citation_strings = cit_map[uri]["citation_strings"]
        # For each citation string build a regex and search in text
        for citation_string in citation_strings:
            prefix = r"\W+[وف]?(?:قال|ذكر)"
            separator = r"\W+"
            regex = prefix + separator + separator.join(citation_string.split(" "))            
            uri_cit_list.append({"citation_uri": uri, "regex": regex})


    results_df = pd.DataFrame()

    # Set a global variable uri_cit_list_corpus for

    if multiprocess:
        # Pass the text_paths to a the function using pool
        print("Processing texts using multiprocessing")

        # Prepare list of tuples as input
        print("Preparing input for multiprocessing...")
        input_data = []
        for text_path in tqdm(text_paths):
            input_data.append((text_path, uri_cit_list))
        
        with Pool(pool_size) as p:
            results_dfs = p.starmap(text_path_to_results, input_data[-150:-100])
        
        results_df = pd.concat(results_dfs)
        
    else:
        for text_path in text_paths[-100:-95]:
            new_results_df = text_path_to_results(text_path, uri_cit_list)
            # print(text_path)
            # verified_path = check_uri_extension(text_path["full_path"])
            # if verified_path is None:
            #     print("No related path found")
            # else:
            #     with open(verified_path, encoding='utf-8-sig') as f:
            #         text = f.read()
            #     new_results = loop_through_ms(text, search_text_for_cits, uri_cit_list, separate_lists=False)
            #     print(len(new_results))            
                
            #     # If using finditer - refactor so that we parse out the different parts of the results dict into columns
            #     # In finditer approach return a list of dicts, so:
            #     for ms in new_results:
            #         new_results_df = pd.DataFrame(ms["result"])
            #         new_results_df["ms"] = int(ms["ms"])
            #         new_results_df["text_uri"] = text_path["book"]
            #         results_df = pd.concat([results_df, new_results_df]) 

            #     # new_results_df = pd.DataFrame(new_results)
            #     # new_results_df["text_uri"] = text_path["book"]
            results_df = pd.concat([results_df, new_results_df])
    
    return results_df
    
    
def infer_source_from_aligned_citation(corpus_citations, verified_citations, cluster_obj, main_text_uri):
    """Function that takes a dataframe of citations in milestones of texts aligned to the main text and uses the alignment to propose a source
    it records the origin for the proposed source and states whether the milestone that is linked precedes the one in which the reuse is aligned (origin_prev_ms)"""
    
    print("Inferring sources from aligned citations...")

    # Fetch the ms clusters for the main text
    main_text_ms = cluster_obj.fetch_ms_for_uri(main_text_uri)

    # Create a concatenated column of uri.ms in corpus_citations - used for lookup
    corpus_citations["uri.ms"] = corpus_citations["text_uri"] + "." + corpus_citations["ms"].astype('int32').astype('str')

    # Prep the verified_citations for addition of new data - by adding origin column
    verified_citations["origin"] = "self"
    verified_citations["origin_prev_ms"] = False

    # Loop through the ms and see if any aligned texts have citations - build df
    for ms in tqdm(main_text_ms):
        ms_clusters = cluster_obj.return_cluster_df_for_uri_ms(main_text_uri, ms)
        ms_clusters = ms_clusters[ms_clusters["book"] != main_text_uri]
        
        # Create a set of uri.ms to lookup in the in the corpus citations - to account for citations preceding the ms - capture a list of preceding too
        ms_clusters_dict = ms_clusters[["book", "seq"]].to_dict("records")
        uri_ms = []
        prev_uri_ms = []
        for ms_cluster in ms_clusters_dict:
            uri_ms.append(ms_cluster["book"] + "." + str(ms_cluster["seq"]))
            prev_uri_ms.append(ms_cluster["book"] + "." + str(ms_cluster["seq"]+1))
        
        # Locate corresponding ms with citations from the clusters
        cited_mss = corpus_citations[corpus_citations["uri.ms"].isin(uri_ms)].to_dict("records")
        cited_mss_prev = corpus_citations[corpus_citations["uri.ms"].isin(prev_uri_ms)].to_dict("records")

        # Build a new df recording this citations and their origins
        new_additions = []
        for cited_ms in cited_mss:
            new_additions.append({
                "uri": cited_ms["uri"],
                "ms": ms,
                "origin": cited_ms["uri.ms"],
                "origin_prev_ms": False 
            })
        for cited_ms_prev in cited_mss_prev:
            new_additions.append({
                "uri": cited_ms_prev["uri"],
                "ms": ms,
                "origin": cited_ms_prev["uri.ms"],
                "origin_prev_ms": True 
            })
        
        add_df = pd.DataFrame(new_additions)
        verified_citations = pd.concat([verified_citations, add_df])
    
    return verified_citations





def analyse_cit_map(cit_map, main_text, cluster_data, meta_path, main_book_uri, corpus_base_path, verified_csv = None, corpus_citations = None, corpus_citations_continuous = None):
    """Experimental functions to try out ways of converting the cit_map into models of lost text use
    Output finding as a reuse map that can be fed into cluster graphing scripts for reuse maps"""

    # Load in json as dict
    with open(cit_map, encoding='utf-8-sig') as f:
        cit_dict = json.load(f)

    # Normalize citation_strings in the cit_dict
    cit_dict = normalize_cit_dict(cit_dict)

    # Load main text
    with open(main_text, encoding='utf-8-sig') as f:
        text = f.read()


    # Fetch text milestones as dictionary of ms:text and convert to df
    ms_dict = loop_through_ms(text)
    ms_df = pd.DataFrame(ms_dict)

    # Create cluster_obj with the minified clusters
    cluster_obj = clusterDf(cluster_data, meta_path)
    

    # Seperate ms that have a verified citation - or follow from a verified citation - this includes texts for which the author can be linked to an author uri, but for which we might posit a lost text
    if verified_csv:
        print("Loading existing verified csv")
        verified_df = pd.read_csv(verified_csv)
    else:        
        unmatched_ms = find_unresolved_ms(ms_df, cit_dict, data_type='cit_map')
        verified_df = identify_continuous_cited_ms(cit_dict, cluster_obj, main_book_uri)
        verified_df.to_csv("outputs_2/verified{}.csv".format(main_book_uri))
    
    # Fetch ms that have no verified source
    unmatched_ms = find_unresolved_ms(ms_df, verified_df)

    # Search aligned text for verified citations
    if not corpus_citations:
        corpus_citations_df = query_cit_map_corpus(main_book_uri, cit_dict, cluster_obj, corpus_base_path, meta_path)
        corpus_citations_df.to_csv("outputs_2/corpus_citations.csv")
    else:
        print("Loading corpus citations...")
        corpus_citations_df = pd.read_csv(corpus_citations)

    # Run corpus_citations through identify_continuous_citations to expand cited segments using reuse evidence
    if corpus_citations_continuous:
        corpus_citations_df = pd.read_csv(corpus_citations_continuous)
    else:
        corpus_citations_df = identify_continuous_corpus_df(corpus_citations_df, cluster_obj)
        corpus_citations_df.to_csv("outputs_2/continuous_corpus_citations.csv")

    # Use clusters and corpus citations to suggest sources for texts
    aligned_cit_df = infer_source_from_aligned_citation(corpus_citations_df, verified_df, cluster_obj, main_book_uri)
    aligned_cit_df.to_csv("outputs_2/citations_with_aligned.csv")

    unmatched_ms = find_unresolved_ms(ms_df, aligned_cit_df)

    # For lost texts with an evaluated citation - look-up corpus citations - create groups of books who had access to the lost text in question


    # Use the library of books associated with citations to predict potential sources for the main text


if __name__ == '__main__':
    cit_map = "citation_resolution/outputs/data/uri_cit_map2.json"
    main_text = "./data/0845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown"
    minified_clusters = "D:/Corpus Stats/2023/v8-clusters/minified_clusters_pre-1000AH_under500_2.csv"
    meta_path = "D:/Corpus Stats/2023/OpenITI_metadata_2023-1-8.csv"
    main_book_uri = "0845Maqrizi.Mawaciz"
    verified_csv = "./outputs_2/verified0845Maqrizi.Mawaciz.csv"
    # corpus_citations = "text_corpus_results.csv"
    corpus_base_path = "D:/OpenITI Corpus/corpus_2023_1_8/"
    # corpus_citations_continuous = "outputs/continuous_corpus_citations.csv"
    analyse_cit_map(cit_map, main_text, minified_clusters, meta_path, main_book_uri, corpus_base_path, verified_csv=verified_csv)