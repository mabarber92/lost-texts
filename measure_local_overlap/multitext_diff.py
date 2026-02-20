from utilities.clusterDf import clusterDf
from py_kitab_diff import kitab_diff
from utilities.openitiTexts import openitiTextMs, openitiCorpus
import pandas as pd


class multitextDiffMap():
    """Take clusters for a defined range of milestones, fetch all clusters across all
    possible pairs (limited by nearest markdown headings)
    Produce a mapping json that will allow for the drawing of a viz showing overlaps and unique sources
    within the source set.
    Approach will only work fully if markdown headings are available, but can force a limit based on an ms boundary"""
    def __init__ (self, meta_tsv, corpus_base_path, cluster_path):
        
        self.cluster_obj = clusterDf(cluster_path, meta_tsv)
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
        print(book_uris)
        for book in book_uris:
            book_data = cluster_df[cluster_df["book"] == book]
            ms_unique = book_data["seq"].drop_duplicates().to_list()
            data_out.append({"book": book, "ms_ranges": self.bin_contiguous(ms_unique)})
        
        return data_out

    def recurse_all_clusters(self, cluster_df, log=False):

        # If the filtering operations at the end of the last process empty the df, then we're done recursing
        if len(cluster_df) == 0:
            return
        
        new_clusters = cluster_df["cluster"].drop_duplicates().to_list()
        self.clusters_checked.extend(new_clusters)

        updated_cluster_df = pd.DataFrame()
        ms_data = self.clusters_to_uri_ms(cluster_df)
        for row in ms_data:
            book = row["book"]
            openiti_text = openitiTextMs(self.openiti_paths[book])
            for ms_range in row["ms_ranges"]:
                new_cluster_df = self.clusters_for_sections(openiti_text, book, ms_range[0], ms_range[-1])
                
                # Remove clusters that we've already checked - so we don't check again
                new_cluster_df = new_cluster_df[~new_cluster_df["cluster"].isin(self.clusters_checked)]



                # Concatenate whatever is left
                updated_cluster_df = pd.concat([updated_cluster_df, new_cluster_df])
        if log:
            self.recurse_log += 1
            print(f"Recursed for {self.recurse_log} time, latest df length: {len(updated_cluster_df)}")
        # Recurse - run the function again with whatever clusters we have left to check (with the new clusters picked up by checking context)
        self.recurse_all_clusters(updated_cluster_df)

        


    def clusters_for_sections(self, openiti_text_obj, book_uri, ms_start, ms_end, nearest_head = "### [|$][^\n]+"):
        """For a given ms range - process all relevant clusters (up to the nearest markdown heading)
        If nearest_head is None, then it will process only the milestone range and all clusters for related milestones"""

       

        if nearest_head is not None:
            # If we have a nearest head - we find the nearest head to the milestone
            # Will need to account for the fact that a supplied ms range might contain multiple headings that we want to retrieve
            # Approach - give first ms - retrieve a ms range (if ms_end not in that we keep stepping until we right the end)
            sections_list = openiti_text_obj.retrieve_md_tags_range(ms_start, ms_end, nearest_head)
        
        else:
            sections_list = [{"tag_text": "Not found", "ms_nos": list(range(ms_start, ms_end+1))}]
        
        # As the recursion may add more sections as it expands need to use an updating strategy
        if book_uri in self.internal_data.keys():
            self.internal_data[book_uri].extend(sections_list)
        else:
            self.internal_data[book_uri] = sections_list
        
 

        # Get a df of all data 
        cluster_df = self.cluster_obj.return_cluster_df_for_uri_ms(book_uri, self.get_uri_ms(book_uri), input_type="list")

        # Remove this book from the df - as we don't want to consider those rows again
        cluster_df = cluster_df[cluster_df["book"] != book_uri]

        return cluster_df
    
    def run_diff_pipeline(self, base_uri, start_ms, end_ms):
        
        # Initiate a place to store ms_ranges used as internal memory across functions - move these down to a pipeline func so we don't accidently store them after running
        self.internal_data = {}
        self.clusters_checked = []
        self.recurse_log = 0

        maintext = openitiTextMs(self.openiti_paths[base_uri])
        initial_df = self.clusters_for_sections(maintext, base_uri, start_ms, end_ms)
        self.recurse_all_clusters(initial_df, log=True)

        print(self.internal_data)