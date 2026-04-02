from openiti.helper.funcs import read_text, text_cleaner
import re
import os
import pandas as pd

class openitiTextMs():
    """A class for handling an OpenITI text as a group of milestones and applying various functions to it"""
    def __init__ (self, file_path, report=False, pre_process_ms=True):
        """Read the text into the object using a file. Store the fulltext and store the milestone splits
        as a special type of dictionary:
        {22: "...كتابة..."}
        On initiation, also create store maximum number of milestones in the text and the zfill level (for text mapping exercises)"""
        
        # Initiate the ms_pattern to be used across the class
        self.ms_pattern = r"ms\d+"

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist")

        # Read in OpenITI text - split off header        
        self.mARkdown_text = read_text(file_path, remove_header=True)
        
        # Run the init pipeline that populates the ms_dict
        if pre_process_ms:
            self.init_process_milestones()

        self.section_map = None

        if report:
            self.report_stats()
        
        self.file_path = file_path
      
    
    def report_stats(self):
        """Read out key stats if they are populated"""
        print(f"Text has a total of: {self.ms_total} milestones")
        print(f"Text milestones zfilled to: {self.zfill_len} characters")

    def is_ms_marker(self, text):
        """Use the specified ms marker to identify if the text that is passed to the function is a ms marker"""
        
        if len(re.findall(self.ms_pattern, text)) == 1:
            return True
        else:
            return False
    

    def fetch_ms_number(self, ms_tag, return_int = True):
        """Take a string and strip the ms from it. If return_int, convert the resulting string into a integer"""
        number = re.split(r"ms", ms_tag)[-1]
        if return_int:
            number = int(number)
        return number

    def check_zfill(self, ms_splits):
        """Take a text split into milestones and splits, find the first milestone marker and use that to calculate
        the zfill (how long is the string used to represent the number)
        It also performs a check - if no ms is found through the whole text, an error is given. As we run this
        as part of the __init__ sequence it also checks the input text has valid formatting for this kind of
        processing"""
        
        # Loop until we hit a valid milestone and use that to get the zfill
        zfill = None
        for ms_split in ms_splits:
            if self.is_ms_marker(ms_split):
                number = self.fetch_ms_number(ms_split, return_int=False)
                zfill = len(number)
                break
        
        # Check that a valid ms has been found and return error if not - if found set the zfill variable
        if zfill is not None:
            self.zfill_len = zfill
        else:
            print("ERROR: Text does not contain a valid milestone splitter, first 5 items split using the ms splitter:")
            print(ms_splits[:5])
            exit()

    def build_ms_dict(self, ms_splits):
        """This is a reusable function - could be adapted to use different templates but for the moment the key is a
        milestone as an integer and the value is the text of the milestone
        Logic: if a split matches the ms marker, then the text preceding the milestone marker is the text for that milestone"""
        
        # Initiate ms_dict
        ms_dict = {}

        # Loop through the ms_splits, if the split is an ms tag, then take the previous list item as the corresponding text
        for idx, ms_split in enumerate(ms_splits):
            
            if self.is_ms_marker(ms_split) and idx > 0:
                ms_text = ms_splits[idx-1]
                ms_int = self.fetch_ms_number(ms_split)
                ms_dict[ms_int] = ms_text
        
        return ms_dict


    def init_process_milestones(self):
        """Take an OpenITI text, initiate key stats about the milestones and populate a dictionary of milestones"""
        
        # Wrap our milestone pattern in brackets so it is included within the splits
        pattern= rf"({self.ms_pattern})"
        ms_splits = re.split(pattern, self.mARkdown_text)

        # Use the splits to get the zfill (and as part of that process check for error in input)
        self.check_zfill(ms_splits)

        # Create the ms dictionary
        self.ms_dict = self.build_ms_dict(ms_splits)
        self.ms_total = len(self.ms_dict)
    

    def fetch_milestone(self, number, clean=False):
        """Use integer to fetch a milestone with that number from the dictionary. If clean, clean using the standard
        OpenITI function (same that is used for passim cleaning - so offsets match)"""
        if type(number) == str:
            number = int(number)
        text = self.ms_dict.get(number)
        if clean and text is not None:
            text = text_cleaner(text)
        if text is None:
            print("Invalid ms given for text. Ms given: {number}")
            exit()
        return text
    
    def calculate_tag_offset_clean(self, ms_number, tag="#{3} [|$][^\nms]+", regex=True):
        """Use a specified tag or search term to calculate a cleaned offset
        Returns cleaned offsets for each place where tag is located"""
        
        tag_offsets = []
        ms_number = int(ms_number)
        ms_text = self.fetch_milestone(ms_number)

        # When looping splits, exclude the last split, so offsets mirror heading pos not length of last section
        if regex:
            splits = re.split(tag, ms_text)
            offset = 0
            for split in splits[:-1]:
                if not re.match(tag, split):
                    
                    offset += len(text_cleaner(split))
                    tag_offsets.append(offset)
        
        else:
            splits = ms_text.split(tag)
            offset = 0
            for split in splits[:-1]:
                offset += len(text_cleaner(split))
                tag_offsets.append(offset)
        
        
        
        return tag_offsets

    def fetch_offset_clean(self, ms_number, start = 0, end = -1, padding=0, trim=0):
        """Clean the ms text using the same OpenITI cleaning process used to pre-process passim inputs
        Return a character offset of the ms text between specified start and end characters. If no start is given
        start from first character of milestone, if no end is given go to the end of the milestone
        padding allows for the adding of a boundary of characters before or after the offset. The padding is
        expanded to the start or end of the nearest token to the start or end +/- padding
        trim is used for attaching context and it trims a set number of characters from the start (to the nearest token)"""
        
        # Ensure arguments are integers
        ms_number = int(ms_number)
        start = int(start)
        end = int(end)

        # Fetch a cleaned version of the milestone text
        text = self.fetch_milestone(ms_number, clean=True)
        


        # If adding padding - find end or start of nearest token to offset - to avoid word splitting
        if padding != 0:
                        
            if end != -1:
                ms_end = len(text)
                end = end + padding
                captured_text = None
                while captured_text != " " and end < ms_end:
                    end += 1
                    captured_text = text[end]
            if start != 0:
                start = start - padding
                captured_text = None
                while captured_text != " " and start > 0:
                    start -= 1
                    captured_text = text[start]
        
        if trim != 0:
            ms_end = len(text)
            start = start + trim
            captured_text = None
            while captured_text != " " and start > 0:
                start-= 1
                captured_text = text[start]

        
        # Make offset
        text = text[start:end]

        return text

    def _check_ms_regex(self, ms_no, regex, return_index=None):
        """Fetch the ms text, check if regex is in the milestone
        return_index: the index of the match to return, if None return all matches as a list
        Returns:
        Tuple: match status (true, false), match (list or list item if return_index)"""
        
        ms_text = self.fetch_milestone(ms_no)
        
        # If the ms_text is not in the dict we return None
        if ms_text is None:
            return True, None

        matches = re.findall(regex, ms_text)
        if len(matches) == 0:
            match_status = False
            match = None
        else:
            match_status = True
            if return_index is not None:
                match = matches[0]
            else:
                match = matches
        return match_status, match


    # def nearest_md_tag(self, ms_no, md_regex, limit=100, approach="backward"):
    #     """Using an ms number iterate backwards or forwards through milestones up to a set limit
    #     until the regex is found in the milestone.
    #     Arguments:
    #     ms_no : the milestone to start from
    #     md_regex: the regex for the md tag (could be any regex in fact)
    #     limit: when to stop iterating - if we hit our limit, return none - if None go until there are no ms left
    #     approach: 'forward' - go to the next milestone and so on until match is found 'backward' go to previous until match found
    #     Returns:
    #     match: if 'forward', the first match in the ms, if 'backward' the last match in the ms
    #     ms_no : the milestone number where the match was found, as an int"""

    #     if approach =="backward":
    #         step = -1
    #         return_index = -1
    #         if limit is None:
    #             end = 0
    #         else:
    #             end = ms_no - limit
    #     if approach == "forward":
    #         step = +1
    #         return_index = 1
    #         if limit is None:
    #             end = len(self.ms_dict)
    #         else:
    #             end = ms_no + limit

    #     match_status = False
    #     # print(f"Starting with {ms_no}")
    #     # print(f"End pos is: {end}")
    #     ms_no = ms_no - step
    #     while not match_status or ms_no == end: # Need to set a way of ending this - this isn't working
    #         ms_no = ms_no + step
    #         match_status, match = self._check_ms_regex(ms_no, md_regex, return_index)
            

            
            
        
    #     return match, ms_no
    
    # def surrounding_md_tags(self, ms_no, md_regex, limit=100):
    #     """Go backwards from an ms and forwards to find the ms start and end boundaries corresponding to
    #     a regex for an md heading
    #     returns:
    #     match: the matching heading at the start of the range (the title of the heading retrieved)
    #     start_ms: the first ms of the range
    #     end_ms: the last ms of the range"""

    #     first_match, start_ms = self.nearest_md_tag(ms_no, md_regex, limit=limit, approach="backward")
    #     # As we accept the given milestone as a location, we have to advanced 1 for one of the checks or we keep checking the same ms over and over
    #     last_match, end_ms = self.nearest_md_tag(ms_no+1, md_regex, limit=limit, approach="forward")

    #     return first_match, start_ms, end_ms
    
    # def retrieve_md_tags_range(self, ms_start, ms_end, md_regex, limit=100):
    #     """For a range of ms retrieve the milestones up to the nearest headings until the
    #     ms range is exhausted
    #     Arguments:
    #     ms_start: the first ms to look backwards from
    #     ms_end: the ms while the search stops - we go until we reach a boundary defined by md_regex
    #     md_regex: the regex for the md tag that defines a section region
    #     Returns:
    #     sections (list of dict) of format:
    #      [{"tag_text": "", "ms_nos": []}] """
        
    #     # If the tag is not in the text at all - just return the ms_start, ms_end range as list
    #     tag_exists = self.check_regex(md_regex)
    #     if not tag_exists:
    #         return [{"tag_text": "Not found", "ms_nos": list(range(ms_start, ms_end+1))}]


    #     sections_list = []
    #     last_found = ms_end-1
    #     while last_found <= ms_end:
    #         tag_text, start_ms, last_found = self.surrounding_md_tags(ms_start, md_regex, limit)
    #         ms_start = last_found
    #         sections_list.append({
    #             "tag_text": tag_text,
    #             "ms_nos": list(range(start_ms, last_found+1))
    #         })
        
    #     return sections_list
    
    def get_ms_count(self):
        last_ms = re.findall(self.ms_pattern, self.mARkdown_text)[-1]
        last_ms = self.fetch_ms_number(last_ms)
        return last_ms
    
    def get_ms_len(self, ms_no):
        return len(self.fetch_milestone(ms_no, clean=True))

    def get_clean_len(self, splits):
        text = "".join(splits)
        cleaned = text_cleaner(text)
        return len(cleaned)

    def fetch_section_offset(self, ms_no, position, ms_head_regex = r"(#{3} [|$][^\nms]+)"):
        """position: 'first' or 'last' - if first take first section if last take last section"""
        # Get ms and split it on the regex - don't clean
        splits = re.split(ms_head_regex, self.fetch_milestone(ms_no))
        # Get the offset position
        # If the split doesn't work - we return the len of the ms if it's in the last pos and zero if first
        if len(splits) == 1:
            if position == "last":
                offset = self.get_ms_len(ms_no)
            if position == "first":
                offset = 0
        else:
            if position == "last":
                offset = self.get_clean_len([splits[0]])
            if position == "first":
                offset = self.get_clean_len(splits[:-1])
        return offset

        


    def get_ms_range_len(self, ms_start, ms_end, get_section_offsets=False):
        """Take a range and return a dict where ms: [start, end], where start always 0 (start of ms)
        If get_section_offsets, check for section boundaries at start and end"""
        ms_lens = {}

        # Handle first and last ms - in case we're getting section offsets
        if get_section_offsets:
            offset_start = self.fetch_section_offset(ms_start, "first")
            offset_end = self.fetch_section_offset(ms_end, "last")
        
        else:
            offset_start = 0
            offset_end = self.get_ms_len(ms_end)
        
        ms_lens[ms_start] = [offset_start, self.get_ms_len(ms_start)]
        ms_lens[ms_end] = [0, offset_end]

        
        # Handle the remaining ms
        for i in range(ms_start+1, ms_end):
            
            ms_len = self.get_ms_len(i)
            ms_lens[i] = [0, ms_len]
        
        return ms_lens



    def find_nearest_section(self, ms_no, last_ms, direction="forwards"):
        """Get the nearest section, going either forwards or backwards"""

        self.ms_head_map()

        if direction == "forwards":
            increment = 1
            head_index = 0
            
        if direction == "backwards":
            increment = -1
            head_index = -1
        
        ms_list = [ms_no]
        while ms_no not in self.section_map.keys() and ms_no != 1 and ms_no <= last_ms:
            ms_no = ms_no + increment
            ms_list.append(ms_no)
        
        return self.section_map.get(ms_no, ["None found"])[head_index], ms_list

    def retrieve_section_for_ms(self, ms_no, last_ms):

        self.ms_head_map()

        section_name, ms_list_before = self.find_nearest_section(ms_no, last_ms, "backwards")
        section_name_after, ms_list_after = self.find_nearest_section(ms_no+1, last_ms, "forwards")

        full_ms_list = list(set(ms_list_before + ms_list_after))

        return section_name, full_ms_list 

    def retrieve_md_tags_range(self, ms_start, ms_end, limit=50):
        """For a range of ms retrieve the milestones up to the nearest headings until the
        ms range is exhausted
        Arguments:
        ms_start: the first ms to look backwards from
        ms_end: the ms while the search stops - we go until we reach a boundary defined by md_regex
        md_regex: the regex for the md tag that defines a section region
        Returns:
        sections (list of dict) of format:
         [{"tag_text": "", "ms_nos": []}]
         Refactor to bring back offsets return: [{"tag_text": "ms_nos": [335], "ms_offsets": {335:[0, 4000] } }] """

        
        
        self.ms_head_map()
        
        # If the ms_head_map produces nothing - then other funcs won't work - just return the ms_range

        if len(self.section_map) == 0:
            # Use ms_nos to populate offsets

            return [{"tag_text": "None found",
                     "ms_nos": list(range(ms_start, ms_end+1)),
                     "ms_offsets": self.get_ms_range_len(ms_start, ms_end)
                     }]
        

        last_ms = self.get_ms_count()

        found_ms = []
        sections_list = []


        for i in range(ms_start, ms_end+1):
            if i in found_ms:
                continue
            # Otherwise this func will capture offsets
            section_name, full_ms_list = self.retrieve_section_for_ms(i, last_ms)
            
            # If we get an ms list that is smaller than our limit, then write it out
            if len(full_ms_list) < limit:

                # Process full_ms_list to get the exact offsets for the start and end and char lens for rest
                full_ms_list.sort()
                
                ms_offsets = self.get_ms_range_len(full_ms_list[0], full_ms_list[-1], get_section_offsets=True)

                sections_list.append({"tag_text": section_name,
                                            "ms_nos": full_ms_list,
                                            "ms_offsets": ms_offsets})
            found_ms.extend(full_ms_list)

        return sections_list




    def check_regex(self, regex, min_results=2):
        """See if a regex applied to the full text returns something to avoid endlessly querying for regex you won't find
        or splitting a text that's not fully annotated (has low result count)"""
        results = re.findall(regex, self.mARkdown_text)
        if len(results) > min_results:
            return True
        else:
            return False

    def ms_head_map(self, ms_head_regex = r"#{3} [|$][^\nms]+", overwrite=False):
        """Produce a section map {ms_no: [head_1, head_2]}"""
        if self.section_map is None or overwrite:

            full_regex = fr"{ms_head_regex}|{self.ms_pattern}"
           
            results_list = re.findall(full_regex, self.mARkdown_text)

            self.section_map = {}

            for idx, result in enumerate(results_list):
                if re.match(ms_head_regex, result):
                    ms_idx = idx
                    while not re.match(self.ms_pattern, results_list[ms_idx]):                    
                        ms_idx += 1
                    ms_no = self.fetch_ms_number(results_list[ms_idx])
                    if ms_no in self.section_map.keys():
                        self.section_map[ms_no].append(result)
                    else:
                        self.section_map[ms_no] = [result] 
        

    def fetch_ms_list_clean(self, ms_list, start=0, end=-1, ms_joins=True, padding=0, trim=0):
        """Take a list of consecutive milestones and return a complete cleaned text according to offsets. start is the offset into the first milestone
        and end is the offset into the last milestone
        ms_joins adds the milestone marker (according to the zfill of in input text) between the milestone boundaries. If set to false then
        the texts are joined without any indication of milestone boundaries"""
        total_idx = len(ms_list) - 1
        final_list = []
        for idx, ms_number in enumerate(ms_list):

            # If it is the first item: take it with the start offset
            if idx == 0:
                text = self.fetch_offset_clean(ms_number, start=start, padding=padding)
            # else if it is the last item: take it with the end offset
            elif idx == total_idx:
                text = self.fetch_offset_clean(ms_number, end=end, padding=padding)
            # otherwise take a whole ms clean
            else:
                text = self.fetch_milestone(ms_number, clean=True)
            
            # Add the ms to the final list
            final_list.append(text)

            # If ms_joins being added, produce the new ms and add it to list
            if ms_joins and idx != total_idx:
                ms_zfill = str(ms_number).zfill(self.zfill_len) 
                ms_string = f"ms{ms_zfill}"
                final_list.append(ms_string)
        
        full_text = "".join(final_list)
        return full_text

class openitiCorpus():
    """Take corpus base path and a metadata tsv, create paths. Perform actions
    on those texts as openITI objects"""
    def __init__ (self, meta_tsv, base_path, language=None, pri_only = True, min_date = 0, max_date = 1500):
        """Initiate with a dictionary of URI-path pairs"""

        meta_df = self.load_and_filter(meta_tsv, language, pri_only, min_date, max_date)

        self.path_dict = self.build_path_dict(meta_df, base_path)
    
    def load_and_filter(self, meta_tsv, language, pri_only, min_date, max_date):
        
        meta_df = pd.read_csv(meta_tsv, sep="\t")
        
        if pri_only:
            meta_df = meta_df[meta_df["status"]=="pri"]
        
        if language is not None and "language" in meta_df.columns:
            meta_df = meta_df[meta_df["language"] == language]
        
        
        meta_df = meta_df[meta_df["date"].ge(min_date)]
        meta_df = meta_df[meta_df["date"].le(max_date)]

        return meta_df
        

    def build_path_dict(self, meta_df, base_path):
        """Take a path to a meta tsv and a OpenITI base path
        Build a dictionary
        Returns: path_dict
        {"bookURI": "path"}
        """




        meta_dict = meta_df[["book", "local_path"]].to_dict("records")
        path_dict = {}

        for meta in meta_dict:
            full_path = os.path.join(base_path, meta["local_path"].split("../")[-1])
            path_dict[meta["book"]] = full_path
        
        return path_dict
    
    def return_path_list(self):
        """Take the values of the path dict and return them as a list of paths"""
        return list(self.path_dict.values())
    
    def fetch_path_for_books(self, book_uris):
        """a list of uris returns a list of paths"""
        if type(book_uris) == str:
            return self.path_dict[book_uris]
        elif type(book_uris) == list:
            file_paths = []
            for book in book_uris:
                file_paths.append(self.path_dict[book])
            return file_paths
    
    def reassign_paths(self, uri_path_dict, allow_new_uris=False):
        """Use a dictionary to map alternative paths to the dictionary
        For use if you want to specify a path to a text that has changed
        since the release (e.g. for improve md or to use tags)
        For this to work paths must be absolute
        Arguments:
        uri_path_dict: {"0000Author.Book": "absolute_path"}
        allow_new_uris: if True, it will allow the addition of a URI not in the dictionary
                        only set to True if your pipeline can handle URIs not in the metadata """
        for uri, path in uri_path_dict.items():
            if allow_new_uris or uri in self.path_dict.keys():
                self.path_dict[uri] = path
            else:
                print("URI not found in the metadata: {uri}") 
                print("Provide a valid URI or set allow_new_uris to True")
                exit()
                



if __name__ == "__main__":

    # Run the class on its own for testing and error checking
    openiti_text_path = "../diff_pipeline_test/corpus//0845Maqrizi.Mawaciz.Shamela0011566-ara1.mARkdown"
    openiti_ms_obj = openitiTextMs(openiti_text_path, report=True)
    
    matching_sections = openiti_ms_obj.retrieve_md_tags_range(1467, 1471)


    print(matching_sections)

    # print("---")
    # print(openiti_ms_obj.fetch_milestone(20))
    # print("---")
    # print(openiti_ms_obj.fetch_ms_list_clean([20,21], start = 20, end=60, ms_joins=False))
    # print("---")
    # print(openiti_ms_obj.fetch_ms_list_clean([20,21], start = 20, end=60))
    # print("---")
    # print(openiti_ms_obj.fetch_ms_list_clean([20,21,22], start = 20, end=60))


