from utilities.clusterDf import clusterDf
from py_kitab_diff import kitab_diff
from utilities.openitiTexts import openitiTextMs, openitiCorpus
from measure_local_overlap.pair_comparison import pairComparison
import pandas as pd
import json
from itertools import combinations
from tqdm import tqdm
from collections import defaultdict
import os

class multitextDiffMap():
    """Take clusters for a defined range of milestones, fetch all clusters across all
    possible pairs (limited by nearest markdown headings)
    Produce a mapping json that will allow for the drawing of a viz showing overlaps and unique sources
    within the source set.
    Approach will only work fully if markdown headings are available, but can force a limit based on an ms boundary"""
    def __init__ (self, meta_tsv, corpus_base_path, cluster_path=None, pairwise_dir=None, uri_text_paths=None):
        """uri_text_paths: a dict mapping book URIs to specific absolute paths to a text - to allow us to drop in a custom annotated text (need to ensure that milestoning still matches the data being used)"""
        if cluster_path == None and pairwise_dir == None:
            print("A cluster_path or pairwise_path must be given. If both are provided then clusters are used for grouping and pairwise for writing diffs")
            exit()

        self.cluster_path = cluster_path
        self.pairwise_dir = pairwise_dir
        self.meta_tsv_path = meta_tsv
        
        self.openiti_paths = openitiCorpus(meta_tsv, corpus_base_path, language="ara")

        if uri_text_paths is not None:
            self.openiti_paths.reassign_paths(uri_text_paths)
            self.openiti_paths = self.openiti_paths.path_dict
        else:
            self.openiti_paths = self.openiti_paths.path_dict

    # If we want to pull out all possible matches for the supplied ranges - we're going to need to recurse - with each new ms range fetched, its possible new clusters will emerge - should go until we exchaust and that will need to account for incomplete md

    def get_uri_ms(self, book_uri, section_dict=None):
        all_ms = set()
        if section_dict is None:
            section_dict = self.internal_data
        for data in section_dict[book_uri]:
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

    def _update_internal_data(self, book_uri, sections_data):
        # As the recursion may add more sections as it expands need to use an updating strategy
        # if book_uri in self.internal_data.keys():
        #     existing_sections = [item["tag_text"] for item in self.internal_data[book_uri]]
        #     for section in sections_data:
        #         if section["tag_text"] not in existing_sections:
        #             self.internal_data[book_uri].append(section)

        # else:
        #     self.internal_data[book_uri] = sections_data
        self.internal_data = self._update_section_data(book_uri, sections_data, self.internal_data)
    
    def _update_section_data(self, book_uri, sections_data, sections_dict):
        # As the recursion may add more sections as it expands need to use an updating strategy
        if book_uri in sections_dict:
            existing_sections = [item["tag_text"] for item in sections_dict[book_uri]]
            for section in sections_data:
                if section["tag_text"] not in existing_sections:
                    sections_dict[book_uri].append(section)

        else:
            sections_dict[book_uri] = sections_data
        
        return sections_dict

    def _filter_pairwise_on_sections(self, sections_data, pairwise_df, book_uri=None, concat_on_df=None):
        """concat_on_df allows to supply existing dataframe to add to"""
        if concat_on_df is None:
            new_pairwise = pd.DataFrame()
        else:
            new_pairwise = concat_on_df
        if book_uri is not None:
            pairwise_df = pairwise_df[pairwise_df["book"] == book_uri] 
        
        for section in sections_data:
            pairwise_update = pairwise_df[pairwise_df["seq"].isin(section["ms_nos"])]
            new_pairwise = pd.concat([new_pairwise, pairwise_update])
        
        new_pairwise = new_pairwise.drop_duplicates()
        
        return new_pairwise

    def _recurse_pairwise(self, section_dict, openiti_dict, full_pairwise, filtered_pairwise, log=False):

        # Remove any b2 rows from the filtered pairwise where the milestone matches what we already have in section_dict
        books = filtered_pairwise["book2"].drop_duplicates().tolist()
        unmatched_data = pd.DataFrame()
        for book in books:
            book_data = filtered_pairwise[filtered_pairwise["book2"] == book]
            if book in section_dict.keys():                
                section_ms = self.get_uri_ms(book, section_dict=section_dict)
                book_data = book_data[~book_data["seq2"].isin(section_ms)]
            
            unmatched_data = pd.concat([unmatched_data, book_data])

        if log:
            self.recurse_log += 1
            print(f"Recursion number: {self.recurse_log}, unmatched data: {len(unmatched_data)} ")
        # If there are no remaining rows of data, we've exhausted the data - close the recursion
        if len(unmatched_data) == 0:
            return section_dict

        # Otherwise For remaining b2 rows - fetch relevant sections - create a new filtered df using those book ms sets (filtering on the b1 side this time)
        new_pairwise = pd.DataFrame()
        remaining_books = unmatched_data["book2"].drop_duplicates().tolist()
        for book in remaining_books:
            openiti_obj = openiti_dict[book]
            book_data = unmatched_data[unmatched_data["book2"] == book]
            ms_bins = self.bin_contiguous(book_data["seq2"].tolist())

            new_book_data = full_pairwise[full_pairwise["book"] == book]

            for ms_bin in ms_bins:
                sections_data = openiti_obj.retrieve_md_tags_range(ms_bin[0], ms_bin[-1])
                new_pairwise = self._filter_pairwise_on_sections(sections_data, new_book_data, new_pairwise)
                section_dict = self._update_section_data(book_uri, sections_data, section_dict)

        new_pairwise = new_pairwise.drop_duplicates()


        # Recurse with the updated filtered_data
        return self._recurse_pairwise(section_dict, openiti_dict, full_pairwise, new_pairwise)

    def pairwise_for_sections(self, book_uri, ms_start, ms_end, nearest_head = "### [|$][^\n]+", log=False):
        """Using a set of pairwise files and a specific ms range for one book in the set, search the pairwise data
        exhaustively to identify all aligned sections"""    
        all_pairwise_df = self._concatenate_pairwise_data(make_bidir=True) # Make bidir and then we only have to query one half of the relationship to fetch the aligned ms

        # Get all of the books in the data and keep unique
        books = all_pairwise_df["book"].tolist()
        books = list(set(books))
        print(books)

        # Get openiti objs for all books in data as dict
        openiti_dict = self.openiti_objs_dict(books)

        # Check the given book uri is in the data
        if not book_uri in books:
            print(f"Given book uri {book_uri} is not in the supplied pairwise data")
            exit()
        
        sections_data = openiti_dict[book_uri].retrieve_md_tags_range(ms_start, ms_end)

        self._update_internal_data(book_uri, sections_data)

        filtered_pairwise = self._filter_pairwise_on_sections(sections_data, all_pairwise_df, book_uri=book_uri)

        # Get all pairs that match with the main ms - will need to do this exchausively - recurse until we capture all pairs for pairs and run out of data
        self.internal_data = self._recurse_pairwise(self.internal_data, openiti_dict, all_pairwise_df, filtered_pairwise, log=log)
        


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
        
        self._update_internal_data(book_uri, sections_list)
        
 
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

    def _map_ms_sections(self, section_ms_dict):
        """Take the dictionary of uri: ["tag_text": "", "ms_nos": []] and
        transform it into {"uri": "ms": "tag_text"}"""
        out_dict = {}
        
        for uri, sections in section_ms_dict.items():
            for section in sections:
                for ms in section["ms_nos"]:
                    if uri in out_dict.keys():
                        out_dict[uri][ms] = section["tag_text"]
                    else:
                        out_dict[uri] = {ms : section["tag_text"]}
        
        return out_dict

    def _fetch_book_from_id(self, data):
        """Function to pass to .apply() to get a book uri from an id"""
        uri_parts = data.split(".")
        book_uri = ".".join(uri_parts[:2])
        return book_uri

    def _concatenate_pairwise_data(self, make_bidir = False):
        pairwise_csvs = os.listdir(self.pairwise_dir)
        # Compile the csvs into one df
        all_unidir = pd.DataFrame()
        for csv in pairwise_csvs:
            csv_path = os.path.join(self.pairwise_dir, csv)
            df = pd.read_csv(csv_path, sep="\t")
            all_unidir = pd.concat([all_unidir, df])
        
        # Create book columns
        all_unidir["book"] = all_unidir["series_b1"].apply(self._fetch_book_from_id)
        all_unidir["book2"] = all_unidir["series_b2"].apply(self._fetch_book_from_id)

        # Drop uneeded data
        all_unidir = all_unidir.drop(columns=["gid", "gid2", "id", "id2", "matches", "s1", "s2", "uid", "uid2", "ch_match", "align_len", "matches_percentage", "w_match", "series_b1", "series_b2"])

        # This bidi function not working - prob need to rename in two stages
        if make_bidir:
            other_dir = all_unidir.copy()
            other_dir = other_dir.rename({
                                                "book": "book2",
                                                "book2": "book",
                                                "begin": "begin2",
                                                "begin2": "begin",
                                                "end": "end2",
                                                "end2": "end",
                                                "seq": "seq2",
                                                "seq2": "seq"
                                                   })
            print("Direction 1:")
            print(all_unidir)
            print("Direction 2:")
            print(other_dir)
            all_pairs = pd.concat([all_unidir, other_dir])
            all_pairs = all_pairs.drop_duplicates()

        else:
            all_pairs = all_unidir

        return all_pairs

    def produce_pairwise_diffs(self):
        # If using pairwise you'll only get a map populated for the pairwise data that have been provided
        # Load all books as openiti_obj - as dict uri: book_obj
        book_uris = list(self.internal_data.keys())

        obj_dict = self.openiti_objs_dict(book_uris)
        ms_sections_map = self._map_ms_sections(self.internal_data)

        

        pairs_data = {}
        # If a pairwise_dir has been given - use existing pairwise data (as it will have more comprehensive offsets)
        if self.pairwise_dir is not None:
            print("Populating map from pairwise data")

            all_unidir = self._concatenate_pairwise_data()

            # Filter the df to get bidir for each book
            for book in tqdm(book_uris):

                # Get a df for cases where book is in the book_1 position         
                book1_df = all_unidir[all_unidir["book"] == book]

                # Get a df for cases where book is in the book_2 position
                book2_df = all_unidir[all_unidir["book2"] == book]

                # Rename columns for book2_df so book is in book_1 position according to our data structure and fields are correctly named - hopefully renamer will handle swaps correctly internally
                book2_df = book2_df.rename(columns= {
                                                    "book2": "book", 
                                                    "seq2": "seq", 
                                                    "begin2": "begin", 
                                                    "end2":"end",
                                                    "seq": "seq2",
                                                    "book": "book2",
                                                    "begin": "begin2",
                                                    "end": "end2"
                                                     })
                # Concatenate two dfs (owing to shared col names book will be in book_1 pos across the data)
                full_df = pd.concat([book1_df, book2_df])
                # Drop duplicates (just in case some bidirectional data slipped into the input data)
                full_df = full_df.drop_duplicates()
                
                # Add to pairs_data
                pairs_data[book] = full_df

        else:
            # Otherwise build pairwise from the clusters
            # Reload clusters - to be sure we've got everything
            print("Populating map from cluster data")
            self.cluster_obj = clusterDf(self.cluster_path, self.meta_tsv_path)

            # Loop through each combination uni-laterally - once a pair is done that's it - get all clusters for that pair and calculate diffs
            # When writing out pairs, we write that out bidirectionally - so we can capture all reuse for each for the map
            # Just fetch data for each b1 - get all the b2s and offsets
            # Could use pairwise data for this - perhaps get a better representation of diff
            
            
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
                                                 "ms2": ms2,
                                                 "section2": ms_sections_map[book_2].get(ms2, "Section outside dict")})
                    section_position += ms_len
        
        return diff_offsets

    def contributor_union_chars_exclusive(self, sub: pd.DataFrame, group_data_by_section=True):
        """
        sub: rows for ONE (book, section)
        Returns: DataFrame with book2, ms2, chars (union length), sorted desc.
        """
        sub = sub.copy()
        if group_data_by_section:
            sub["rid"] = list(zip(sub["book2"], sub["section2"]))
        else:
            sub["rid"] = list(zip(sub["book2"], sub["ms2"]))
        sub = sub.sort_values(["rid", "start", "end"])

        sub["run_end"] = sub.groupby("rid")["end"].cummax()
        prev = sub.groupby("rid")["run_end"].shift(1)

        # touching merges (start == prev) => same cluster; new only if start > prev
        sub["new"] = prev.isna() | (sub["start"] > prev)
        sub["rid_cluster"] = sub.groupby("rid")["new"].cumsum().astype(int)

        unions = (sub.groupby(["rid", "rid_cluster"], as_index=False)
                    .agg(start=("start","min"), end=("end","max")))

        unions["chars"] = unions["end"] - unions["start"]  # exclusive end ✅

        contrib = (unions.groupby("rid", as_index=False)["chars"].sum()
                        .sort_values("chars", ascending=False))

        contrib[["book2","ms2"]] = pd.DataFrame(contrib["rid"].tolist(), index=contrib.index)
        return contrib.drop(columns=["rid"])
    

    def make_patches_exclusive(self, sub: pd.DataFrame, group_data_by_section=True):
        """
        sub: rows for ONE (book, section)
        Returns: list of dict patches with start, end, intensity
                where intensity = number of unique (book2, ms2) covering [start, end)
        """
        sub = sub.copy()
        # If set to true build "rid" out of the section title to use that to amalgamate
        if group_data_by_section:
            sub["rid"] = list(zip(sub["book2"], sub["section2"]))
        else:
            sub["rid"] = list(zip(sub["book2"], sub["ms2"]))

        starts = defaultdict(list)
        ends = defaultdict(list)

        for s, e, rid in sub[["start", "end", "rid"]].itertuples(index=False, name=None):
            s = int(s); e = int(e)
            if e <= s:
                continue
            starts[s].append(rid)
            ends[e].append(rid)

        points = sorted(set(starts) | set(ends))
        if len(points) < 2:
            return []

        active = set()
        patches = []

        # Sweep boundaries; starts-before-ends gives "touching counts as overlap" behavior.
        for i, x in enumerate(points[:-1]):
            for rid in starts.get(x, []):
                active.add(rid)

            next_x = points[i + 1]
            if next_x > x and active:
                patches.append({
                    "start": x,
                    "end": next_x,
                    "intensity": len(active),
                    # optional if you want later drill-down per patch:
                    # "active": sorted(active),
                })

            for rid in ends.get(x, []):
                active.discard(rid)

        return patches

    def build_mapping_dictionary(self, pairwise_df, group_data_by_section=True):
        # drop unnamed index if present
        # pairwise_df = pairwise_df.drop(columns=[c for c in pairwise_df.columns if c.startswith("Unnamed")], errors="ignore")

        out = {}
        for (book, section), sub in pairwise_df.groupby(["book", "section"], sort=False):
            patches = self.make_patches_exclusive(sub, group_data_by_section=group_data_by_section)
            contrib = self.contributor_union_chars_exclusive(sub, group_data_by_section=group_data_by_section)

            out.setdefault(book, {})[section] = {
                "patches": patches,
                "contributors": contrib.to_dict("records"),
            }
        return out    



    def build_multi_diff_map(self, group_data_by_section=True):
        """Take the internal data and use it to build a diff map
        This pass only takes diffs on passim-aligned data - we could
        expand to explore shared gaps too"""

        # Produce the pairwise diffs
        pairwise_map = self.produce_pairwise_diffs()
        df = pd.DataFrame(pairwise_map)
        df.to_csv("outputs_check.csv")

        # Following code - from ChatGPT - creates a dictionary mapping the overlaps - need to update to ensure we feed in sections in order in which they're in the book
        mapping_dict = self.build_mapping_dictionary(df, group_data_by_section=group_data_by_section)

        return mapping_dict

        # Concatenate pairs and write to map at char level

    def _export_metadata_mapping(self, out_dir):
        """Create a csv that allows for manual mapping of metadata to uri, section fields
        returns two csvs: one for section mappings and one for uri mappings"""

        # Build mappings
        uri_meta = []
        sections_meta = []
        for uri, sections in self.internal_data.items():
            uri_meta.append({"uri": uri,
                             "meta": ""})
            for section in sections:
                sections_meta.append({"uri": uri,
                                      "section": section["tag_text"],
                                      "meta": ""})

        # Create paths
        uri_path = os.path.join(out_dir, "uri_meta.csv")
        sections_path = os.path.join(out_dir, "sections_meta.csv")
        
        # Convert dict lists to df and export
        pd.DataFrame(uri_meta).to_csv(uri_path, encoding='utf-8-sig')
        pd.DataFrame(sections_meta).to_csv(sections_path, encoding='utf-8-sig')

    def _export_data(self, mapping_dict, out_dir):
        """Perform all exports once data has been created"""

        print("Writing data")
        if not os.path.exists(out_dir):
            os.mkdir(out_dir)
        
        self._export_metadata_mapping(out_dir)

        mapping_json_path = os.path.join(out_dir, "verbatim_mapping.json")
        self.write_json(mapping_dict, mapping_json_path) 


    def run_diff_pipeline(self, base_uri, start_ms, end_ms, out_dir, max_recursions=None, log=False):
        
        # Initiate a place to store ms_ranges used as internal memory across functions - move these down to a pipeline func so we don't accidently store them after running
        self.internal_data = {}
        self.clusters_checked = []
        self.recurse_log = 0

        if self.cluster_path is not None:
            # As the pipeline run will shrink the data as it goes - initiate a new cluster_obj with the pipeline
            self.cluster_obj = clusterDf(self.cluster_path, self.meta_tsv_path)
            
            # On later runs we're checking ms over and over - need to clean out ms we've already checked
            maintext = openitiTextMs(self.openiti_paths[base_uri], pre_process_ms=False)
            initial_df = self.clusters_for_sections(maintext, base_uri, start_ms, end_ms)
            self.recurse_all_clusters(initial_df, log=log, max_recursions=max_recursions)
        
        else:
            self.pairwise_for_sections(base_uri, start_ms, end_ms, log=log)
            
        
        mapping_dict = self.build_multi_diff_map()
        self._export_data(mapping_dict, out_dir)

        # # Add func to export a csv meta template (uri, sections, translation) - to allow for easy label customisation in graph
        # output_data = []
        # for key, value in self.internal_data.items():
        #     output_data.append({"book_uri": key, "ms_sections": value})
        # self.write_json(output_data, "test_data.json")
        