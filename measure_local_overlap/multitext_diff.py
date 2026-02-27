from utilities.clusterDf import clusterDf
from py_kitab_diff import kitab_diff
from utilities.openitiTexts import openitiTextMs, openitiCorpus
from measure_local_overlap.pair_comparison import pairComparison
import pandas as pd
import json
from itertools import combinations
from tqdm import tqdm

class multitextDiffMap():
    """Take clusters for a defined range of milestones, fetch all clusters across all
    possible pairs (limited by nearest markdown headings)
    Produce a mapping json that will allow for the drawing of a viz showing overlaps and unique sources
    within the source set.
    Approach will only work fully if markdown headings are available, but can force a limit based on an ms boundary"""
    def __init__ (self, meta_tsv, corpus_base_path, cluster_path):
        
        self.cluster_path = cluster_path
        self.meta_tsv_path = meta_tsv
        
        self.openiti_paths = openitiCorpus(meta_tsv, corpus_base_path, language="ara").path_dict


    # If we want to pull out all possible matches for the supplied ranges - we're going to need to recurse - with each new ms range fetched, its possible new clusters will emerge - should go until we exchaust and that will need to account for incomplete md

    def get_uri_ms(self, book_uri):
        all_ms = set()
        for data in self.internal_data[book_uri]:
            all_ms.update(data["ms_nos"])
        return list(all_ms)

    def bin_contiguous(self, nums):
        nums = sorted(nums) 
        if not nums:
            return []

        groups = [[nums[0]]]
        for x in nums[1:]:
            if x == groups[-1][-1] + 1:
                groups[-1].append(x)
            else:
                groups.append([x])
        return groups

    def clusters_to_uri_ms(self, cluster_df):
        """Take a cluster_df and return a list of dicts of book_uri, ms list,
        as lengths of continuous spans. We need ms ranges to avoid over determining and fetching too much data
        Returns:
        [{"book": "", "ms_ranges": [[100, 101, 102], [200, 201, 202]]}] 
        """
        data_out = []
        book_uris = cluster_df["book"].drop_duplicates().to_list()
        for book in book_uris:
            book_data = cluster_df[cluster_df["book"] == book]
            ms_unique = book_data["seq"].drop_duplicates().to_list()
            data_out.append({"book": book, "ms_ranges": self.bin_contiguous(ms_unique)})
        
        return data_out

    # Prior to recursion - filter on alignment length or count - remove insignificant cases from recursion entirely so we don't keep growing

    def recurse_all_clusters(self, cluster_df, max_recursions=None, log=False):

        # If the filtering operations at the end of the last process empty the df, then we're done recursing
        if len(cluster_df) == 0:
            return None
        if max_recursions is not None:
            if self.recurse_log == max_recursions:
                return cluster_df
        
        new_clusters = cluster_df["cluster"].drop_duplicates().to_list()
        self.clusters_checked.extend(new_clusters)

        updated_cluster_df = pd.DataFrame()
        ms_data = self.clusters_to_uri_ms(cluster_df)
        for row in ms_data:
            
            book = row["book"]


            openiti_text = openitiTextMs(self.openiti_paths[book], pre_process_ms=False)
            for ms_range in row["ms_ranges"]:
                # print(f"Book: {book}, range start: {ms_range[0]}, range_end {ms_range[-1]}")
                
                # Need to check if ms already in list - overwise we get re-gen for each independent range that's part of same section
                new_cluster_df = self.clusters_for_sections(openiti_text, book, ms_range[0], ms_range[-1])
                
                # Remove clusters that we've already checked - so we don't check again
                new_cluster_df = new_cluster_df[~new_cluster_df["cluster"].isin(self.clusters_checked)]



                # Concatenate whatever is left
                updated_cluster_df = pd.concat([updated_cluster_df, new_cluster_df])
        if log:
            self.recurse_log += 1
            print(f"Recursion number: {self.recurse_log}, latest df length: {len(updated_cluster_df)}")
            print(self.internal_data)
        # Recurse - run the function again with whatever clusters we have left to check (with the new clusters picked up by checking context)
        self.recurse_all_clusters(updated_cluster_df, log=log, max_recursions=max_recursions)

        


    def clusters_for_sections(self, openiti_text_obj, book_uri, ms_start, ms_end, nearest_head = "### [|$][^\n]+"):
        """For a given ms range - process all relevant clusters (up to the nearest markdown heading)
        If nearest_head is None, then it will process only the milestone range and all clusters for related milestones"""

       

        if nearest_head is not None:
            # If we have a nearest head - we find the nearest head to the milestone
            # Will need to account for the fact that a supplied ms range might contain multiple headings that we want to retrieve
            # Approach - give first ms - retrieve a ms range (if ms_end not in that we keep stepping until we right the end)
            sections_list = openiti_text_obj.retrieve_md_tags_range(ms_start, ms_end)
        
        else:
            sections_list = [{"tag_text": "Not found", "ms_nos": list(range(ms_start, ms_end+1))}]
        
        # As the recursion may add more sections as it expands need to use an updating strategy
        if book_uri in self.internal_data.keys():
            existing_sections = [item["tag_text"] for item in self.internal_data[book_uri]]
            for section in sections_list:
                if section["tag_text"] not in existing_sections:
                    self.internal_data[book_uri].append(section)

        else:
            self.internal_data[book_uri] = sections_list
        
 
        ms_list = self.get_uri_ms(book_uri)
        
        # Get a df of all data 
        cluster_df = self.cluster_obj.return_cluster_df_for_uri_ms(book_uri, ms_list, input_type="list")
        
        # Filter the original cluster df to ensure we don't treat the same clusters twice
        self.cluster_obj.remove_clusters_by_uri_ms(book_uri, ms_list)

        # Remove this book from the df - as we don't want to consider those rows again
        cluster_df = cluster_df[cluster_df["book"] != book_uri]

        return cluster_df
    
    def write_json(self, data, export_path, indent=2):
        json_string = json.dumps(data, ensure_ascii=False, indent=indent)
        with open(export_path, "w", encoding='utf-8') as f:
            f.write(json_string)   

    def openiti_objs_dict(self, uri_list):
        """Take a list of book URIs and create OpenITI objs with ms dicts already initialised,
        ready for fetching ms and offsets"""
        print("Loading mARkdown text data")
        obj_dict = {}
        for uri in tqdm(uri_list):
            uri_path = self.openiti_paths[uri]
            obj_dict[uri] = openitiTextMs(uri_path)
        return obj_dict

    def create_unidir_pairs(self, uri_list):
        """Take a URI list and create a unidirectional list of tuples representing
        pairs of books"""
        pairs = list(combinations(uri_list, 2))
        return pairs

    def _update_pairs_data(self, key_name, pairs_data, update_data):
        
        if key_name in pairs_data.keys():
            pairs_data[key_name].extend(update_data)
        else:
            pairs_data[key_name] = update_data
        
        return pairs_data

    def produce_pairwise_diffs(self):
        
        # Load all books as openiti_obj - as dict uri: book_obj
        book_uris = list(self.internal_data.keys())
        obj_dict = self.openiti_objs_dict(book_uris)

        # Reload clusters - to be sure we've got everything
        self.cluster_obj = clusterDf(self.cluster_path, self.meta_tsv_path)

        # Loop through each combination uni-laterally - once a pair is done that's it - get all clusters for that pair and calculate diffs
        # When writing out pairs, we write that out bidirectionally - so we can capture all reuse for each for the map
        # Just fetch data for each b1 - get all the b2s and offsets
        # Could use pairwise data for this - perhaps get a better representation of diff
        
        pairs_data = {}
        print("Fetching pairwise data")
        for book_1 in tqdm(book_uris):
            book_2_list = book_uris.copy()
            book_2_list.remove(book_1)
            # print(book_1)
            # print(f"Book 2s: {book_2_list}")
            book_1_ms = self.get_uri_ms(book_1)
            clusters = self.cluster_obj.return_cluster_df_for_uri_ms(book_1, book_1_ms, input_type="list")[["book", "seq", "begin", "end", "cluster"]]

            b2_clusters = clusters[clusters["book"].isin(book_2_list)][["book", "cluster", "seq", "begin", "end"]]
            b2_clusters = b2_clusters.rename(columns= {"book": "book2", "seq": "seq2", "begin": "begin2", "end": "end2"})
            clusters = clusters[clusters["book"] == book_1]
            pairs = pd.merge(clusters, b2_clusters, on="cluster", how="inner")
            pairs_data[book_1] = pairs
        
        diff_offsets = []

        # Now we have paired data - we run diffs and convert to absolute section-based offsets - at pairwise level - loop through books and sections in book to create output
        print("Writing section adjusted offsets")
        for book in tqdm(book_uris):
            for section in self.internal_data[book]:
                
                openiti_obj_b1 = obj_dict[book] 
                section_ms = sorted(section["ms_nos"])
                section_title = section["tag_text"]
                first_ms = section_ms[0]
                last_ms = section_ms[-1]
                if section_title == "None found":
                    start_offset = 0
                    end_offset = len(openiti_obj_b1.fetch_milestone(last_ms, clean=True))
                else:
                    if first_ms == last_ms:
                        start_offset = openiti_obj_b1.calculate_tag_offset_clean(first_ms, section_title, regex=False)[0]
                        end_offset = openiti_obj_b1.calculate_tag_offset_clean(last_ms)[-1]
                    else:
                        # In case there are multiple headings that match in the ms, we take the last match, as that what was used to determine the heading in the first place
                        
                        start_offset = openiti_obj_b1.calculate_tag_offset_clean(first_ms, section_title, regex=False)[-1]
                        # We take the first match to determine the end
                        

                        end_offset = openiti_obj_b1.calculate_tag_offset_clean(last_ms)[0]
                
                # The start offset tells us what data to exclude from first ms, end offset tells us what to exclude from end offset
                # To get an offset into the section we need to calculate cumulative offsets and augment - for later ordering, need to store first ms in output
                # Note - b/c we returned pairwise bi-dir data - we're calculating these diffs twice once for each direction - a little expensive
                section_position = 0
                for ms in section_ms:
                    pairwise_data = pairs_data[book]                    
                    pairwise_data = pairwise_data[pairwise_data["seq"] == ms]
                    ms_len = len(openiti_obj_b1.fetch_milestone(ms, clean=True))
                
                    if ms == first_ms:
                        pairwise_data = pairwise_data[pairwise_data["end"] > start_offset]
                        # Make the augmentation negative if it's the first ms
                        offset_augment = 0 - start_offset
                    elif ms == last_ms:
                        pairwise_data = pairwise_data[pairwise_data["begin"] < end_offset]
                        offset_augment = section_position
                    else:
                        offset_augment = section_position
                    
                    pairwise_data = pairwise_data.to_dict("records")
                    for data in pairwise_data:
                        # print(data)
                        book_2 = data["book2"]
                        ms2 = data["seq2"]
                        text_a = openiti_obj_b1.fetch_offset_clean(ms, start= data["begin"], end=data["end"])
                        text_b_obj = obj_dict[data["book2"]]
                        
                        text_b = text_b_obj.fetch_offset_clean(ms2, start=data["begin2"], end=data["end2"])
                        offset_data = pairComparison(text_a, text_b).fetch_verbatim_offsets(augment_offset_a= offset_augment, return_text=False)
                        for offset in offset_data["offsets_a"]:
                            diff_offsets.append({"section": section_title,
                                                "book": book,
                                                 "start": offset["start"],
                                                 "end": offset["end"],
                                                 "first_ms": first_ms,
                                                 "book2": book_2,
                                                 "ms2": ms2})
                    section_position += ms_len
        
        return diff_offsets

                    


    def build_multi_diff_map(self):
        """Take the internal data and use it to build a diff map
        This pass only takes diffs on passim-aligned data - we could
        expand to explore shared gaps too"""

        # Produce the pairwise diffs
        pairwise_map = self.produce_pairwise_diffs()
        df = pd.DataFrame(pairwise_map)
        df.to_csv("outputs_check.csv")


        # Concatenate pairs and write to map at char level

    def run_diff_pipeline(self, base_uri, start_ms, end_ms, max_recursions=None):
        
        # Initiate a place to store ms_ranges used as internal memory across functions - move these down to a pipeline func so we don't accidently store them after running
        self.internal_data = {}
        self.clusters_checked = []
        self.recurse_log = 0

        # As the pipeline run will shrink the data as it goes - initiate a new cluster_obj with the pipeline
        self.cluster_obj = clusterDf(self.cluster_path, self.meta_tsv_path)
        
        # On later runs we're checking ms over and over - need to clean out ms we've already checked
        maintext = openitiTextMs(self.openiti_paths[base_uri], pre_process_ms=False)
        initial_df = self.clusters_for_sections(maintext, base_uri, start_ms, end_ms)
        self.recurse_all_clusters(initial_df, log=True, max_recursions=max_recursions)
        self.build_multi_diff_map()
        
        output_data = []
        for key, value in self.internal_data.items():
            output_data.append({"book_uri": key, "ms_sections": value})
        self.write_json(output_data, "test_data.json")
        