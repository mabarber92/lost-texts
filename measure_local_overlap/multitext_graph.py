import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Rectangle
import matplotlib.patches as mpatches
from matplotlib.collections import PatchCollection
from collections import Counter
import math
from tqdm import tqdm
import numpy as np
import colorsys

class multitextGraph():

    def __init__ (self, mapping_json, uri_meta=None, section_meta=None, log=False, uri_filter=None):
        self.mapping_dict = self.load_json(mapping_json)

        # Set the default graph spacing
        self.set_spacing_parameters()


        # # metadata_csv allows to provide transliterations or translations of the data and URI mappings
        # # if given, only the uris in the meta are used for the analysis
        # if metadata_csv is not None:
        #     self.meta_df = pd.read_csv(metadata_csv)
        #     # Filter the data
        #     # Map the metadata into the json
        # else:
        #     self.meta_df = None
        #     self.meta = None
        
        
        if uri_filter is not None:
            self.filter_uris(uri_filter)



        if uri_meta is not None:
            self._map_metadata(uri_meta, section_meta)
        else:
            self.uri_map = None
        
        # Get the highest number of characters and number of books
        self._get_summary_data()

        # Set up log
        self.log=log
        self.patch_log = []
        self.df_log=0

        self._priority_index = {}
        

    def load_json(self, json_path):

        with open(json_path, encoding='utf-8') as f:
            data = json.load(f)
          
        return data
    
    def filter_uris(self, uri_list):
        new_dict = {}
        for uri in uri_list:
            new_dict[uri] = self.mapping_dict[uri]
        print(f"Data filtered from {len(self.mapping_dict)} to {len(new_dict)} uris")
        self.mapping_dict = new_dict
        self._get_summary_data()

    # Function to get max chars
    def _get_summary_data(self):
        print("Getting summary data")
        max_chars = 0
        max_intensity = 0
        max_sections = 0
        max_contributors = 0
        all_combinations = []
        for book, data in self.mapping_dict.items():
            book_contributors = 0
            for section, section_data in data.items():
                book_contributors += len(section_data["contributors"])
            if book_contributors > max_contributors:
                max_contributors = book_contributors
            book_max = 0
            book_intensity_max = 0
            book_sections = len(data.keys())  # ADD THIS
            if book_sections > max_sections:  # ADD THIS
                max_sections = book_sections  # ADD THIS
            for section, section_data in data.items():                
                res = Counter()
                for patches in section_data["patches"]:
                    # Hack to fix group-by issues in input data
                    # print(patches)
                    # if "active" in patches.keys():
                    #     active = patches["active"]
                    #     print(active)
                    #     if type(active[0]) == list:
                    #         joined_items = []
                    #         for item in active:
                    #             new_item = item[0]
                    #             print(new_item)
                    #             joined_items.append(new_item)
                    #         print(joined_items)
                    #         joined_items = list(set(joined_items))
                    #         patches["active"] = joined_items
                    #         patches["intensity"] = len(joined_items)

                    for k, v in patches.items():
                        
                        
                        if type(v) == int:
                            res[k] = max(res[k], v)
                        if k == "active":
                            # Handle cases where sections outputted into active with book title
                            
                            all_combinations.append(patches[k])                
                book_max += section_data["char_total"]
                if res["intensity"] > book_intensity_max:
                    book_intensity_max = res["intensity"]
            
               
                
            if book_max > max_chars:
                max_chars = book_max
            if book_intensity_max > max_intensity:
                max_intensity = book_intensity_max
        
        # Using all possible combinations create a set of unique combinations
        self.unique_combos = sorted(set(
            tuple(sorted(combo)) for combo in all_combinations
        ))
        
                
        self.max_sections = max_sections
        self.max_contributors = max_contributors
        self.all_combinations = all_combinations
        self.max_chars = max_chars
        self.max_intensity = max_intensity
        self.book_count = len(self.mapping_dict.keys())

    def _match_meta(self, df, datapoint, matching_field, meta_field="meta"):
        matching_data = df[df[matching_field] == datapoint].dropna().to_dict("records")
        if len(matching_data) > 0:
            text = matching_data[0][meta_field]
            if len(text) == 0:
                text = datapoint
        else:
            text= datapoint
        
        return text

    def _resolve_uri_meta(self, uri_list):
        """Take a uri_list and resolve them to a list of corresponding uri metadata"""
        if self.uri_map is None:
            return uri_list
        else:
            mapped_list = []
            for uri in uri_list:
                mapped_list.append(self.uri_map[uri])
            return mapped_list

    def _map_metadata(self, uri_csv, section_csv=None):
        """Take metadata mapping files and modify the incoming data to have metadata rather than original data"""
        
        print("Mapping metadata")
        uri_df = pd.read_csv(uri_csv)
        if section_csv is not None:
            section_df = pd.read_csv(section_csv)
        else:
            # to avoid lots of nested ifs - use empty dataframe - which _meta_meta will force to populate with existing data
            section_df = pd.DataFrame()
        
        # For use elsewhere in the script - create a metadata mapping
        self.uri_map = {}

        new_mapping_dict = {}
        print(self.mapping_dict.keys())
        for book, data in tqdm(self.mapping_dict.items()):
            
            book_meta = self._match_meta(uri_df, book, "uri")
            self.uri_map[book] = book_meta
            new_data = {}            
            for section, section_data in data.items():                
                section_meta = self._match_meta(section_df, section, "section")

                for book2_data in section_data["contributors"]:
                    book2_meta = self._match_meta(uri_df, book2_data["book2"], "uri")
                    book2_data["book2"] = book2_meta

                for patch_data in section_data["patches"]:
                    new_active = []
                    for active_book in patch_data["active"]:
                        active_meta = self._match_meta(uri_df, active_book, "uri")
                        new_active.append(active_meta)
                    patch_data["active"] = new_active
   
                        
                
                new_data[section_meta] = section_data
           
            new_mapping_dict[book_meta] = new_data

        self.mapping_dict = new_mapping_dict
        print(self.mapping_dict.keys())
        





    def _calculate_line_length(self, max_lines=None, chars_per_line=None, max_chars=None):
        if max_chars is None:
            max_chars = self.max_chars
        if max_lines is not None:
            line_length = math.ceil(max_chars/max_lines)
        if chars_per_line is not None:
            max_lines = math.ceil(max_chars/chars_per_line)
            line_length = chars_per_line
        return line_length, max_lines

    def _create_patches_df(self, book_section_patches):
        """For book sections dictionary - take the patches and create a df - augment the char count
        Add rows for the section annotations"""
        
        char_pos = 0
        df_out = pd.DataFrame()
        for section, data in book_section_patches.items():
            
            # Round up the char_pos to the nearest section start (to avoid starting in the middle of a row)
            if char_pos != 0:
                nearest_line = math.ceil(char_pos/self.line_length)
                char_pos = (nearest_line + 1) * self.line_length

            
            # Prepare the annotation row
            annot_end = char_pos+self.line_length
            annotation = [{"start": char_pos,
                           "end": annot_end,
                           "intensity": 0,
                           "type": "annotation",
                           "label": section}]
            annotation_df = pd.DataFrame(annotation)
            
            
            # Augment the char pos in the patches to account for current char pos + annotation row
            char_pos += self.line_length
            patches_df = pd.DataFrame(data["patches"])
            patches_df["type"] = "reuse"
            patches_df["label"] = None

            patches_df["start"] += char_pos
            patches_df["end"] += char_pos

            # Prepare the start row
            start_row = [{"start": annot_end,
                         "end": patches_df["start"].min(),
                        "intensity": 0,
                        "type": "start",
                        }]
            start_df = pd.DataFrame(start_row)
            
            # Prepare the end row
            end_row = [{"start": patches_df["end"].max(),
                         "end": data["char_total"] + char_pos,
                        "intensity": 0,
                        "type": "end",
                        }]
            end_df = pd.DataFrame(end_row)

            df_out = pd.concat([df_out, annotation_df, start_df, patches_df, end_df])
            
            char_pos = df_out["end"].max() 
            
        self.df_log += 1
        if self.log:
            df_out.to_csv(f"test_patch_df-{self.df_log}.csv")

        return df_out

    def _add_patch_data(self, start, width, current_height, height_increase, patch_list, color_list,  intensity = None, color=None, wrap=None):
            """Function to update patch lists"""
            


            
            patch = self._create_rectangle(start, width, current_height, height_increase)
            patch_list.append(patch)
            # Write intensity or color
            if intensity is not None:
                color_list.append(intensity)
            # elif color is not None:
            #     color_list.append(color)
            
            if self.log:
                log_data = {"start": start, "width": width, "height": current_height, "height_increase": height_increase, "color": color, "intensity": intensity, "data": wrap}
                self.patch_log.append(log_data)

            return patch_list, color_list



    def _write_data_to_patch(self, start_offset, end_offset, current_wrap, current_height, height_increase, column_pos, intensity=None, color=None):
        """Take a data point and write as many patches as needed to deal with the length of the given data point
        To use for empty lines - give start_offset as 0 and end_offset as line length
        Returns: list of written patches, list of related colors or intensities, depending on which is given
        new_height, new_wrap"""
        
        # Add the char pos to offsets - so we don't use them twice in our filter
        self.checked_chars.extend(list(range(start_offset, end_offset)))

        patches = []
        colors = []
        # Offsets still not working - creating a staggered result.
        # Set the start pos using the current wrap + column_pos (which column are we in) - with all additional cols we're getting stuck in infinite loop
        start_pos = current_wrap + column_pos
        # print(f"Start pos: {start_pos}")
        # print(f"Line max: {self.line_length + column_pos}")
        width = end_offset - start_offset
        # Write patches until the width is less than line length
        
        
        while width + (start_pos - column_pos) >= self.line_length:
            patch_len = self.line_length + column_pos - start_pos # This fix is the issue
            patches, colors = self._add_patch_data(start_pos, patch_len, current_height, height_increase, patches, colors, intensity, color, [current_wrap, start_offset, end_offset])
            
            
            # Update width by removing patch_len (giving us remainder) - if width is longer than patch_len
            if width >= patch_len:
                width -= patch_len
            
            # Update the start_pos to start of column - as we'll carry the wrap at the very end
            start_pos = column_pos
            # Augment current height
            current_height += height_increase
            
        
        # Once the width is down to under the line length, write out the final patch with remaining width if it's above 0
        if width > 0:
            patches, colors = self._add_patch_data(start_pos, width, current_height, height_increase, patches, colors, intensity, color, [current_wrap, start_offset, end_offset])
            
        # # Increase the height ready for next pass
        # current_height += height_increase
        # The new wrap is the outstanding width
        new_wrap = start_pos + width - column_pos

        return patches, colors, new_wrap, current_height


    def _normalise_combo(self, combo):
        if self._priority_index:
            return tuple(sorted(combo, key=lambda b: (self._priority_index.get(b, float('inf')), b)))
        else:
            return tuple(sorted(combo))

    def _set_intensity(self, data=None):
        if self.mode == "sequential":
            if data is None:
                return 0
            else:
                return data["intensity"]
        elif self.mode == "categorical":
            if data is None:
                return tuple([None])
            elif type(data["active"]) == list:
                return self._normalise_combo(data["active"])  # was: tuple(sorted(data["active"]))
            else:
                print("Appropriate data type not found for a categorical map in:")
                print(data)
                exit()


    def _write_patches(self, max_lines, nonreuse_color = "lightgrey", annotation_gap=30, annotate_stats=True, book_order = None):
        """Create a list of rectangular patches using the line length and the data dict"""
        # Will need to return the locations for labels alongside the gaps (or even just an annotation-compliant format that we can pass to matplotlib)
        print("Writing patches...")
        height_increase = 1 # A bit basic - multiplying the increase by the book count

        # Set annotation gap as a unit of height increases - so that it's uniform
   
        patches_list = []
        patch_intensity = []
        gap_patches_list = []
        gap_colors = []
        section_boxes = []
        
        annotations_list = []
  
        horizontal_markers = []

        max_book_lines = []

        heatmap_log = {}
        
        # Set horizontal position at zero - will increase by char len + 1 with each book
        horizontal_pos = 0
        # If a book_order has been set, use that as an order for a book, otherwise just take dict items
        if book_order is not None:
            books = book_order[:]
            books = self._resolve_uri_meta(books)
        else:
            books = self.mapping_dict.keys()
        # Loop through each book
        for book in tqdm(books):
            
            section_ended=False

            data = self.mapping_dict[book]
            heatmap_log[book] = {}
            # Reset the checked chars for each book
            self.checked_chars = []
            
            # Reset height
            height = 0
            # Set initial wrap (to handle wrapping)
            wrap = 0
            # For each book add new lines for each section - this should only be calculated on max chars in the book (or it'll fill out beyond length of section - this fix still needed)

            
            # Convert the data into a df for easy filtering - but need to handle having gap or annotation for the diff sections
            data_df = self._create_patches_df(data)

            # Df above takes account of metadata for section lens - if that data exists - so we just take max end of that data
            max_book_chars = data_df["end"].max() # Use this as the maximum for the range
            
            # Use that data to calculate maximum number of lines for that book - just for ylim
            line_length, book_lines = self._calculate_line_length(chars_per_line=self.line_length, max_chars=max_book_chars)
            book_lines = book_lines + (len(data.keys()) *annotation_gap)
            
            
            


            # # To check all data has been added to patches and for duplicate data
            # patched_data = pd.DataFrame()

            # Add book title to annotation        
            height += (annotation_gap*self.title_gap)
            wrap = 0                      
            print(f"Added title at line: {height}")
            # annot_count +=1
            # if annot_count > 1:
            #     exit()
            
            # Add annotation to our annotations list
            annotation = {
                "label_text": book,
                "y": height-(annotation_gap*self.title_height), 
                "x": horizontal_pos+self.line_length-1,
                "va": "top",
                "font_multiple": 1.2
            }
            annotations_list.append(annotation)

            
            # Loop up to max_chars with steps of line_length
            for i in range(0, max_book_chars, self.line_length):
                
                # Calculate the line end - deduct 1 to avoid overlap and double counting
                line_end = i + self.line_length
                # Filter the df for the relevant sections
                # Fetch a list of chars to filter by
                char_list = list(range(i, line_end))
                # Filter out checked chars
                filter_chars = char_list.copy()
                for char in char_list:
                    if char in self.checked_chars:
                        filter_chars.remove(char)
                # print(f"Char list: {char_list}, Filter chars: {filter_chars}")
                # If resulting filter list empty - continue - to avoid adding duplicate gap lines
                if len(filter_chars) == 0:
                    
                    continue
                # Filter starts
                line_data = data_df[data_df["start"].isin(filter_chars)]
                # print(line_data)
                # # Filter ends
                # line_data = line_data[line_data["end"].isin(filter_chars)]
                # print(line_data)

                
                # If line_data is empty, then add a noreuse_color to the empty line and reset the wrap to zero
                if len(line_data) == 0:
                    
                    if nonreuse_color is not None:
                        intensity = self._set_intensity()
                        new_gap_patches, new_intensities, wrap, height = self._write_data_to_patch(i, line_end, wrap, height, height_increase, horizontal_pos, intensity=intensity)
                        patches_list.extend(new_gap_patches)
                        patch_intensity.extend(new_intensities)
                        heatmap_log[book][0] = heatmap_log[book].get(0, 0) + line_end-i
                    
        

                else:
                # If only annotation in the row skip and just reset overlap and height - as check, throw error if annotation and data in the row
                    # patched_data = pd.concat([patched_data, line_data])
                    if section_ended:
                        # If we've reached the end of a section add an additional 2 to the height after completing all relevant patches    
                        height += self.section_gap*height_increase
                        section_ended=False
                    types = line_data["type"].drop_duplicates().tolist()
                    if "annotation" in types and "reuse" not in types:
                        
                        
                        height += annotation_gap * self.section_title_gap
                        wrap = 0                      
                        print(f"Added annotation at line: {height}")
                        # annot_count +=1
                        # if annot_count > 1:
                        #     exit()
                        
                        # Add annotation to our annotations list
                        data = line_data.to_dict("records")[0]
                        annotation = {
                            "label_text": data["label"],
                            "y": height- (self.section_title_adjust*height_increase), 
                            "x": horizontal_pos+self.line_length-3,
                            "va": "bottom"
                        }
                        annotations_list.append(annotation)
                        
                        # Set section start height for wrapping box
                        section_start = height-annotation_gap+height_increase*self.section_box_top
                    
                    # This is an error check - if logic of _create_patches_df correct, then this shouldn't trip
                    elif "annotation" in types and "reuse" in types:
                        print("Offset alignment is misaligned - mixed annotation and data in a row")
                        print(f"Current offset: {i}")
                        print("Filtered dataframe:")
                        print(line_data)
                    else:
                        # Else - loop through the data and produce the patch rectangles
                        listed_data = line_data.sort_values(by=["start"]).to_dict("records")
                        row_count = len(listed_data)
                        for idx, row in enumerate(listed_data):

                            if row["type"] == "end" or row["type"] == "start":
                                print(f"Writing patch for {row['type']}")
                                intensity = self._set_intensity()
                                new_patches, new_intensities, wrap, height = self._write_data_to_patch(row["start"], row["end"], wrap, height, height_increase, horizontal_pos, intensity=intensity)
                                patch_intensity.extend(new_intensities)
                                heatmap_log[book][0] = heatmap_log[book].get(0, 0) + row["end"]- row["start"]

                                # If 'end' Set section end and write out section_patch
                                if row["type"] == "end":
                                    section_height = height+(self.section_box_bottom*height_increase) - section_start
                                    section_box = self._create_rectangle(horizontal_pos, self.line_length, section_start, section_height, section_box=True)
                                    section_boxes.append(section_box)
                                    section_ended = True
                                    
                                    

                            # Write the data to a patch - the function will handle lines longer than line length and wrap
                            else:
                                intensity = self._set_intensity(row)
                                new_patches, new_intensities, wrap, height = self._write_data_to_patch(row["start"], row["end"], wrap, height, height_increase, horizontal_pos, intensity = intensity)
                                patch_intensity.extend(new_intensities)
                                heatmap_log[book][row["intensity"]] = heatmap_log[book].get(row["intensity"], 0) + row["end"]- row["start"]
                            
                            patches_list.extend(new_patches)
                            
                            # If nonreuse_color is not None - check if there is a gap between this offset and the next one - if there is create a gap patch
                            if nonreuse_color is not None and idx < row_count-1:
                                next_start = listed_data[idx+1]["start"]
                                
                                if next_start - row["end"] > 0:
                                    intensity = self._set_intensity()
                                    new_gap_patches, new_intensities, wrap, height = self._write_data_to_patch(row["end"], next_start, wrap, height, height_increase, horizontal_pos, intensity=intensity)
                                    patches_list.extend(new_gap_patches)
                                    patch_intensity.extend(new_intensities)
                                    heatmap_log[book][0] = heatmap_log[book].get(0, 0) + next_start - row["end"]

                        
            
            # Add the annotation stats to the bottom of the diff
            if annotate_stats:
                height += self.annot_gap * height_increase
                annotation_text = "Aligned books:"
                annotations_list.append({"label_text": annotation_text,
                    "y" : height,
                    "x" : horizontal_pos+self.line_length-3,
                    "va": "top",
                    "font_multiple": 1
                    })
                height += annotation_gap*self.annot_title_space

                concatenated_stats = {}           
                for section, book_data in self.mapping_dict[book].items():
                    
                    # Concatenate the data
                    for book2 in book_data["contributors"]:
                        # If metadata has been given - fetch from pre-processed metadata
                        book = book2["book2"]
                        concatenated_stats[book] = concatenated_stats.get(book, 0) + book2['chars']

                        
                for book, stat in concatenated_stats.items():
                    annotation_text = []
                    label = [f"{book} - {stat} chars"]
                    annotation_text.extend(label)

                    annotation_text = "\n".join(annotation_text)
                
                    annotations_list.append({"label_text": annotation_text,
                                        "y" : height,
                                        "x" : horizontal_pos+self.line_length-3,
                                        "va": "top",
                                        "font_multiple": 0.7
                                        })
                    height += annotation_gap*self.annot_spacing

            max_book_lines.append(height)
            # Update horizontal for next book - use + 1 to create a gap
            
            horizontal_pos += self.line_length + 4
            horizontal_markers.append(horizontal_pos)

            

            # Check data validity and export results
            # missed_data = data_df.merge(patched_data, how="left", indicator=True).query('_merge == "left_only"')
            # print(f"{len(missed_data)} rows of data missing for book {book}")
            # missed_data.to_csv(f"{book}_missed_rows.csv")
            # duplicate_data = patched_data[patched_data.duplicated(keep=False)]
            # print(f"{len(duplicate_data)} rows duplicated or more for book {book}")
            # duplicate_data.to_csv(f"{book}_duplicate_data.csv")
                  
        
        # Create a collection from the patches
        if self.log:
            print(heatmap_log)
        

        
        
        patch_collection = PatchCollection(patches_list, cmap=self.cmap, norm=self.norm, edgecolor='none')
        
        self._apply_patch_colors(patch_collection, patch_intensity)
        # if self.mode == "categorical":
        #     combo_index = {combo: i+1 for i, combo in enumerate(self.unique_combos)}
        #     mapped = [combo_index.get(v, 0) for v in patch_intensity]
        #     patch_collection.set_array(np.asarray(mapped))
        #     patch_collection.set_clim(0, len(self.unique_combos) + 1)  # fix the scale globally

        # else:
        #     patch_collection.set_array(np.asarray(patch_intensity))
        #     patch_collection.set_clim(0, self.max_intensity)

        
        section_collection = PatchCollection(section_boxes, match_original=True)

        # Set max lines to max of list of lines
        self.max_lines = max(max_book_lines)

        # Create a separate patch collection for the gaps
        gap_patch_collection = PatchCollection(gap_patches_list, facecolors=gap_colors)

        # Return the patch collections and the annotation
        return patch_collection, gap_patch_collection, annotations_list, horizontal_markers, section_collection
    
    def _apply_patch_colors(self, patch_collection, patch_intensity):
        if self.mode == "categorical":
            mapped = [self.combo_index.get(v, 0) for v in patch_intensity]
        else:
            mapped = patch_intensity
        patch_collection.set_array(np.asarray(mapped))
        patch_collection.set_clim(0, len(self.cmap.colors)-1 if self.mode == "categorical" else self.max_intensity)

    def _set_horizontal_position(self, data_offset, absolute_pos, horizontal_pos, wrap):
        """Calculate the position of an offset based on the current position in the 
        data (absolute_pos) and the horizontal_pos of the graph and adjustment for wrapping"""
        
        position = horizontal_pos + wrap + absolute_pos + self.line_length - data_offset
        
        return position

    def _create_rectangle(self, start, width, current_height, height_increase, section_box=False, data=None):
        """Use data about start, end position and height to create a rectangle using that data"""
        
        xy = (start, current_height)
        # width = end-start
        # if width > self.line_length or width < 0:
        #     print(width)

        if section_box:
            linestyle = "-"
            linewidth = 0.5
            
        else:
            linestyle = None
            linewidth = None
            

        rect = Rectangle(xy, width, height_increase, facecolor='none', linestyle=linestyle, edgecolor='black', linewidth=linewidth)
        return rect

    # def _reorder_unique_combinations(self, lead_books):
    #     """Use a set of lead books to create a set of custom
    #     combinations prioritising order of lead books"""

    #     def check_combination(book, combo, index, new_combinations, checked_combinations):
    #         sorted_combo = combo.sort()
    #         if combo[index] == book and sorted_combo not in checked_combinations:
    #             new_combinations.append(tuple(combo))
    #             checked_combinations.append(sorted_combo)
    #         return new_combinations, checked_combinations
            

    #     new_combinations = []
    #     checked_combinations = []
    #     for book in lead_books:
    #         print(book)
    #         for combo in self.all_combinations:
    #             for i in range(len(combo)):
    #                 new_combinations, checked_combinations = check_combination(book, combo, i, new_combinations, checked_combinations)
                    

    #     self.unique_combos = new_combinations
    #     print(self.unique_combos)
    
    def _sort_combos_by_priority(self, priority_order):
        priority_index = {book: i for i, book in enumerate(priority_order)}

        def normalise(combo):
            # Canonical within-combo order: by priority, then alphabetically
            return tuple(sorted(combo, key=lambda b: (priority_index.get(b, float('inf')), b)))

        def get_lead_and_key(combo):
            hits = [(priority_index[b], b) for b in combo if b in priority_index]
            if hits:
                lead_priority, lead_book = min(hits)
            else:
                lead_priority = len(priority_order)
                lead_book = combo[0]
            return (lead_priority, lead_book, len(combo), combo)

        # Deduplicate using priority-normalised form (not alphabetical)
        normalised = sorted(set(normalise(c) for c in self.all_combinations))
        return sorted(normalised, key=get_lead_and_key)

    def _build_categorical_colors(self, base_map=None, cat_order=None):
        """Build a custom categorical colour set that allows for intuitive reading of category combinations"""
        
        # If a category order is specified, then the lead books are populated from that
        if cat_order:
            self.unique_combos = self._sort_combos_by_priority(cat_order)
            self._priority_index = {book: i for i, book in enumerate(cat_order)}
            def get_lead(combo):
                hits = [(self._priority_index[b], b) for b in combo if b in self._priority_index]
                return min(hits)[1] if hits else combo[0]
        else:
            self.unique_combos = sorted(set(tuple(sorted(c)) for c in self.all_combinations))
            self._priority_index = {}
            def get_lead(combo):
                return combo[0]
        

        # Group combos by lead to calculate shade steps
        from collections import defaultdict
        lead_groups = defaultdict(list)
        for combo in self.unique_combos:
            lead_groups[get_lead(combo)].append(combo)
        
        # Decide colormap strategy
        max_group_size = max((len(g) for g in lead_groups.values()), default=1)
        use_tab20b = base_map is None and max_group_size <= 3
        
        # Assign one base hue per unique lead book, in the order they first appear
        seen_leads = []
        for combo in self.unique_combos:
            lead = get_lead(combo)
            if lead not in seen_leads:
                seen_leads.append(lead)

        lead_hue_index = {lead: i for i, lead in enumerate(seen_leads)}
        
        # If parameters met and no specific map given - tab20c as a default
        if use_tab20b:
            base_index = 0
            tab20b = plt.get_cmap("tab20c")
            # tab20c is laid out as 5 hue groups * 4 shades, light to dark
            # Each group of 4 starts at indices 0, 4, 8, 12, 16
            color_list = [(0.85, 0.85, 0.85, 1)]
            self.combo_index = {}
            for combo in self.unique_combos:
                lead = get_lead(combo)
                group = lead_groups[lead]
                hue_idx = lead_hue_index[lead]
                shade_idx = group.index(combo)
                
                # Each hue block is 4 colours starting at hue_idx * 4
                # shade_idx 0 = lightest, 3 = darkest - add 4 and deduct to go from lighter to darker shades
                color = tab20b.colors[hue_idx * 4 + 2 - shade_idx]
                color_list.append(color)
                self.combo_index[combo] = len(color_list) - 1
            return mcolors.ListedColormap(color_list)    


        # Default for colour mapping if number larger
        if base_map is None:
            base_map = "tab10"
        base_hues = plt.get_cmap(base_map)
        # Index 0 = no-reuse grey
        color_list = [(0.85, 0.85, 0.85, 1)]
        self.combo_index = {}



        max_group_size = max(len(g) for g in lead_groups.values())

        for combo in self.unique_combos:
            lead = get_lead(combo)
            group = lead_groups[lead]
            shade_idx = group.index(combo)

            # Different shading logic
            # base = base_hues(lead_hue_index[lead] / 10.0)  # RGBA
    
            # # Convert to HLS, darken by reducing lightness only
            # r, g, b = base[0], base[1], base[2]
            # h, l, s = colorsys.rgb_to_hls(r, g, b)
            
            # light_ceiling = min(0.65, l + 0.25)  # relative to base, not absolute
            # sat_floor = 0.3  # how desaturated the lightest shade gets

            # t = shade_idx / max(max_group_size - 1, 1)  # 0.0 = lightest, 1.0 = base
            # l_new = light_ceiling + t * (l - light_ceiling)
            # s_new = sat_floor + t * (s - sat_floor)

            # r_new, g_new, b_new = colorsys.hls_to_rgb(h, l_new, s_new)
            # color_list.append((r_new, g_new, b_new, 1.0))
            # self.combo_index[combo] = len(color_list) - 1

            # Old shading logic
            base = np.array(base_hues(lead_hue_index[lead]))
            factor = 1.0 - 0.5 * (shade_idx / max(max_group_size - 1, 1))
            color_list.append(tuple(base[:3] * factor) + (1.0,))
            self.combo_index[combo] = len(color_list) - 1

        for lead, group in lead_groups.items():
            print(f"{lead}: {len(group)} combos")

        return mcolors.ListedColormap(color_list)

    def _set_color_mapping(self, color_map=None, cat_order=None):
        """ Initiate a sequential color heatmap - using max intensity as top of scale"""
        if self.mode == "sequential":
            print(f"Max intensity: {self.max_intensity}")
            if color_map is None:
                color_map="Greys"
            base_cmap = plt.get_cmap(color_map)

            self.cmap = mcolors.LinearSegmentedColormap.from_list(
                    "truncated", base_cmap(np.linspace(0.15, 1.0, 256))
            )
            self.norm = mcolors.Normalize(vmin=0, vmax=self.max_intensity)
            
        if self.mode == "categorical":
            if cat_order is not None:
                cat_order = self._resolve_uri_meta(cat_order)
            
            self.cmap = self._build_categorical_colors(base_map=color_map, cat_order=cat_order)
            n = len(self.cmap.colors)
            self.norm = mcolors.NoNorm()
            # Above suggested - below worked before
            # self.norm = mcolors.BoundaryNorm(boundaries=range(n + 1), ncolors=n)
            
            


    def _fetch_color_mapping(self, intensity):
        """Function to allow less verbose code for passing the color to the patches"""
        return self.cmap(self.norm(intensity))
    
    def _wrap_text_to_data_width(self, ax, text, x, y, max_width_xdata, fontsize=12, **text_kwargs):
        """
        Solution from ChatGPT to wrap on the actual data width - if xlim or ylim changed, must re-wrap
        Wrap `text` so that each line fits within `max_width_xdata` (x-axis data units)
        when drawn at (x, y) on the given Axes.

        Returns a string with newline breaks inserted.
        """
        fig = ax.figure

        # Ensure we have a renderer (needed to measure text)
        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()

        # Convert data-width (x units) -> pixels
        x0_disp, _ = ax.transData.transform((x, y))
        x1_disp, _ = ax.transData.transform((x + max_width_xdata, y))
        max_width_px = abs(x1_disp - x0_disp)

        words = text.split()
        if not words:
            return ""

        lines = []
        current = words[0]

        # A temporary text artist for measuring
        t = ax.text(
            0, 0, "", fontsize=fontsize, alpha=0.0,  # invisible
            transform=ax.transAxes,  # irrelevant for measuring width
            **text_kwargs
        )

        for w in words[1:]:
            candidate = f"{current} {w}"
            t.set_text(candidate)
            bbox = t.get_window_extent(renderer=renderer)

            if bbox.width <= max_width_px:
                current = candidate
            else:
                lines.append(current)
                current = w

        lines.append(current)
        t.remove()  # clean up

        return "\n".join(lines)

    def _add_col_annotations(self, ax, annotation_list, font_size=9):
        base_font_size = math.ceil((font_size/self.book_count)*3)
        print(f"Font size for {self.book_count} books is {font_size}")
        for annotation in annotation_list:
            if "font_multiple" in annotation.keys():
                font_size = base_font_size * annotation["font_multiple"]
            else:
                font_size = base_font_size
            wrapped = self._wrap_text_to_data_width(ax, annotation["label_text"], annotation["x"], annotation["y"], self.line_length , fontsize=font_size)
            ax.text(annotation["x"], annotation["y"], wrapped, size = font_size, wrap=True, va=annotation["va"]
                     )

    def _build_legend(self, ax, patch_collection, book_joiner= " ; ", font_size=9):
        # Add a colorbar or legend
        if self.mode == "categorical":
            col_count = math.ceil(len(self.unique_combos)/3)
            handles = [
                mpatches.Patch(
                    facecolor=self.cmap(self.norm(self.combo_index[combo])),
                    label=book_joiner.join(combo)  # or however you want to format the combo label
                )
                for combo in self.unique_combos
            ]
            handles.insert(0, mpatches.Patch(facecolor=self.cmap(self.norm(0)), label="No reuse"))
            ax.legend(
                handles=handles,
                loc="upper center",
                bbox_to_anchor=(0.5, -0.01),
                fontsize=font_size,
                title_fontsize = font_size*0.9,
                title="Aligned Books",
                borderaxespad=0,
                ncols=col_count
            )
            
        else:
            plt.colorbar(patch_collection, ax=ax, aspect=40, ticks=list(range(0, self.max_intensity+1)))

    def set_spacing_parameters(self, title_gap=2, title_height=1.25, section_gap=4, 
        section_title_gap=1, section_title_adjust=3, section_box_top = 1, section_box_bottom=3, annot_gap=4,
        annot_title_space = 0.5, annot_spacing=0.35):
        """Function that sets the spacing globally - applied at init with defaults - can be adjusted prior to drawing"""
        # Multiples of annotation gap
        self.title_gap = title_gap
        self.title_height = title_height
        self.section_title_gap = section_title_gap
        self.annot_title_space = annot_title_space
        self.annot_spacing = annot_spacing

        # Multiples of height
        self.section_gap = section_gap
        self.section_title_adjust = section_title_adjust
        self.section_box_top = section_box_top
        self.section_box_bottom = section_box_bottom
        self.annot_gap = annot_gap
    
    def apply_spacing_parameters(self, fig, ax, figsize, max_lines, font_size):
        
        fig_height_pixels = figsize[1] * fig.dpi
        
        k = (
            self.title_gap +
            self.max_sections * self.section_title_gap +
            self.annot_title_space +
            self.annot_spacing * self.max_contributors
        )
        fixed = (
            self.annot_gap +
            self.max_sections * self.section_gap +
            self.explan_space
        )
        
        estimated_ylim = (max_lines + fixed) / (1 - (font_size * 2 * k) / fig_height_pixels)
        
        ax.set_xlim((self.line_length + 4) * self.book_count, 0)
        ax.set_ylim(estimated_ylim, 0)

        fig.canvas.draw()
        y0 = ax.transData.inverted().transform((0, 0))[1]
        y1 = ax.transData.inverted().transform((0, 1))[1]
        data_units_per_pixel = abs(y1 - y0)
        data_units_per_point = data_units_per_pixel * (fig.dpi / 72)

        return data_units_per_point
    # Overall graphing function
    def draw_diff_graph(self, max_lines=500, chars_per_line=None, color_map = None, export_path=None, add_explan=True, map_type="heatmap",
        book_order = None, cat_order=None, figsize=None, font_size=9):
        """Main func for drawing the graph - max lines is the number of lines to go to for the longest book"""
        self.line_length, max_lines = self._calculate_line_length(max_lines, chars_per_line)
        print(f"max_lines: {max_lines}")
        
        # Set the mode to be used by the cmap, patch builder and collection mapper
        if map_type == "heatmap":
            self.mode = "sequential"
        else:
            self.mode = "categorical"
        
        if add_explan:
            self.explan_space = 10
        else:
            self.explan_space = 0
        
        # if cat_order is not None:
        #     cat_order = self._resolve_uri_meta(cat_order)
        #     self.unique_combos = self._reorder_unique_combinations(cat_order)

        self._set_color_mapping(color_map, cat_order)

        # annotation_gap - changes depending on font size and chars-per-line - this might need to have a col adjust too later
            # Create figure FIRST so we can use transforms
        if figsize is None:
            px = 1/plt.rcParams['figure.dpi']
            figsize = (1200*px, 800*px)
        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(1, 1, 1)

        # Set axis limits before transform — use estimated ylim from max_lines
        # This will be corrected after _write_patches
        # annotation_contribution = (
        #     annotation_gap * 2 +                                    # title
        #     annotation_gap * self.max_sections +                    # section labels
        #     6 +                                                     # stats header fixed gap
        #     annotation_gap / 3 +                                    # "Aligned books:" label
        #     annotation_gap / 4 * self.max_contributors              # per stat line
        # )
        # estimated_ylim = max_lines + annotation_contribution + 10 # for bottom of graph
        # fig_height_pixels = figsize[1] * fig.dpi
        # k = 3 + self.max_sections + 1/3 + self.max_contributors / 4 + 10
        # estimated_ylim = (max_lines + 4) / (1 - (font_size * 2 * k) / fig_height_pixels)
        
        # ax.set_xlim((self.line_length+4) * self.book_count, 0)
        # ax.set_ylim(estimated_ylim, 0)

        # # Now get stable data-units-per-point using the transform
        # fig.canvas.draw()
        # y0 = ax.transData.inverted().transform((0, 0))[1]
        # y1 = ax.transData.inverted().transform((0, 1))[1]  # 1 pixel up
        # data_units_per_pixel = abs(y1 - y0)
        # data_units_per_point = data_units_per_pixel * (fig.dpi / 72)
        data_units_per_point = self.apply_spacing_parameters(fig, ax, figsize, max_lines, font_size)
        # annotation_gap is now font_size * 4 expressed in stable physical units
        annotation_gap = font_size * 2 * data_units_per_point
        


        patch_collection, gap_patch_collection, annotation_list, horizontal_markers, section_boxes = self._write_patches(max_lines=max_lines, book_order = book_order, annotation_gap=annotation_gap)
        print(horizontal_markers)
        
        # if figsize is None:
        #     px = 1/plt.rcParams['figure.dpi']  # pixel in inches
        #     figsize = (1200*px, 800*px)
        # fig = plt.figure(figsize=figsize)
        # ax = fig.add_subplot(1, 1, 1)
        
        ax.add_collection(patch_collection)
        ax.add_collection(section_boxes)
        # ax.add_collection(gap_patch_collection)
        
        print(f"annotation gap: {annotation_gap}")
        print(f"chars per line: {chars_per_line}")
        print(f"ylim: {self.max_lines}")
        

        # Set axis as flipped - so that we have rtl and top to bottom display
        ax.set_xlim((self.line_length+4) * self.book_count, 0)     # or your max end offset
        
        ax.set_ylim(self.max_lines+10, 0) 

        # Add annotations - below xlim and ylim to allow for wrapping
        self._add_col_annotations(ax, annotation_list, font_size = font_size)

        # Add the correct legend
        self._build_legend(ax, patch_collection, font_size=font_size)
                
        ax.set_axis_off()
        
        # Add annotation to bottom for number of chars per row
        if add_explan:
            annotation_text = f"A heatmap of verbatim overlap between {self.book_count} books, where each vertical line is equivalent to {self.line_length} characters"
            ax.text((self.line_length+4) * self.book_count, self.max_lines+10, annotation_text, size=font_size*0.9)

        if self.log:
            log_df = pd.DataFrame(self.patch_log)
            log_df.to_csv("patch_log.csv")
            print(self.max_intensity)

        if export_path is not None:
            fig.savefig(export_path, bbox_inches="tight", dpi=300)
        else:
            plt.show()
