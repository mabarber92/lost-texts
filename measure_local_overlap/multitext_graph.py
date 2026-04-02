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

class multitextGraph():

    def __init__ (self, mapping_json, uri_meta=None, section_meta=None, log=False, uri_filter=None):
        self.mapping_dict = self.load_json(mapping_json)


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
        
        # Get the highest number of characters and number of books
        self._get_summary_data()

        # Set up log
        self.log=log
        self.patch_log = []
        self.df_log=0
        

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
        all_combinations = []
        for book, data in self.mapping_dict.items():
            book_max = 0
            book_intensity_max = 0
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
        print(self.unique_combos)
                

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

    def _map_metadata(self, uri_csv, section_csv=None):
        """Take metadata mapping files and modify the incoming data to have metadata rather than original data"""
        
        print("Mapping metadata")
        uri_df = pd.read_csv(uri_csv)
        if section_csv is not None:
            section_df = pd.read_csv(section_csv)
        else:
            # to avoid lots of nested ifs - use empty dataframe - which _meta_meta will force to populate with existing data
            section_df = pd.DataFrame()
        
        new_mapping_dict = {}
        print(self.mapping_dict.keys())
        for book, data in tqdm(self.mapping_dict.items()):
            
            book_meta = self._match_meta(uri_df, book, "uri")
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
                return tuple(sorted(data["active"]))
            else:
                print("Appropriate data type not found for a categorical map in:")
                print(data)
                exit()


    def _write_patches(self, max_lines, nonreuse_color = "lightgrey", annotation_gap=30, annotate_stats=True):
        """Create a list of rectangular patches using the line length and the data dict"""
        # Will need to return the locations for labels alongside the gaps (or even just an annotation-compliant format that we can pass to matplotlib)
        print("Writing patches...")
        height_increase = +1 # A bit basic - multiplying the increase by the book count
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
        # Loop through each book
        for book, data in tqdm(self.mapping_dict.items()):
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
            height += height_increase*(annotation_gap*1.5)
            wrap = 0                      
            print(f"Added title at line: {height}")
            # annot_count +=1
            # if annot_count > 1:
            #     exit()
            
            # Add annotation to our annotations list
            annotation = {
                "label_text": book,
                "y": height-20, 
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
                    types = line_data["type"].drop_duplicates().tolist()
                    if "annotation" in types and "reuse" not in types:
                        
                        
                        height += height_increase*annotation_gap
                        wrap = 0                      
                        print(f"Added annotation at line: {height}")
                        # annot_count +=1
                        # if annot_count > 1:
                        #     exit()
                        
                        # Add annotation to our annotations list
                        data = line_data.to_dict("records")[0]
                        annotation = {
                            "label_text": data["label"],
                            "y": height-4, 
                            "x": horizontal_pos+self.line_length-3,
                            "va": "bottom"
                        }
                        annotations_list.append(annotation)
                        
                        # Set section start height for wrapping box
                        section_start = height-(annotation_gap/1.5)
                    
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
                                    section_height = height+2 - section_start
                                    section_box = self._create_rectangle(horizontal_pos, self.line_length, section_start, section_height, section_box=True)
                                    section_boxes.append(section_box)

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
                height += 4
                annotation_text = "Aligned books:"
                annotations_list.append({"label_text": annotation_text,
                    "y" : height,
                    "x" : horizontal_pos+self.line_length-3,
                    "va": "top",
                    "font_multiple": 0.7
                    })
                height += annotation_gap/3

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
                                        "font_multiple": 0.6
                                        })
                    height += annotation_gap/4

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

    def _build_categorical_colors(self, base_map="tab10"):
        """Build a custom categorical colour set that allows for intuitive reading of category combinations"""
        lead_books = sorted(set(combo[0] for combo in self.unique_combos))
        base_hues = plt.get_cmap(base_map)
        
        # Index 0 reserved for no-reuse
        color_list = [(0.85, 0.85, 0.85, 1)]
        self.combo_index = {}
        
        for book_idx, lead in enumerate(lead_books):
            group = [c for c in self.unique_combos if c[0] == lead]
            for shade_idx, combo in enumerate(group):
                base = np.array(base_hues(book_idx))
                factor = 1.0 - 0.5 * (shade_idx / max(len(group) - 1, 1))
                color_list.append(tuple(base[:3] * factor) + (1.0,))
                self.combo_index[combo] = len(color_list) - 1
        print(color_list)
        return mcolors.ListedColormap(color_list)

    def _set_color_mapping(self, color_map="Greys"):
        """ Initiate a sequential color heatmap - using max intensity as top of scale"""
        if self.mode == "sequential":
            print(f"Max intensity: {self.max_intensity}")
            base_cmap = plt.get_cmap(color_map)

            self.cmap = mcolors.LinearSegmentedColormap.from_list(
                    "truncated", base_cmap(np.linspace(0.15, 1.0, 256))
            )
            self.norm = mcolors.Normalize(vmin=0, vmax=self.max_intensity)
            
        if self.mode == "categorical":
            
            self.cmap = self._build_categorical_colors()
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
            wrapped = self._wrap_text_to_data_width(ax, annotation["label_text"], annotation["x"], annotation["y"], self.line_length*0.75 , fontsize=font_size)
            ax.text(annotation["x"], annotation["y"], wrapped, size = font_size, wrap=True, va=annotation["va"]
                     )

    def _build_legend(self, ax, patch_collection, book_joiner= " ; "):
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
                fontsize=6,
                title="Aligned texts",
                borderaxespad=0,
                ncols=col_count
            )
            
        else:
            plt.colorbar(patch_collection, ax=ax, aspect=40, ticks=list(range(0, self.max_intensity+1)))


    # Overall graphing function
    def draw_diff_graph(self, max_lines=500, chars_per_line=None, color_map = "YlOrBr", export_path=None, add_explan=True, map_type="heatmap"):
        """Main func for drawing the graph - max lines is the number of lines to go to for the longest book"""
        self.line_length, max_lines = self._calculate_line_length(max_lines, chars_per_line)
        
        
        # Set the mode to be used by the cmap, patch builder and collection mapper
        if map_type == "heatmap":
            self.mode = "sequential"
        else:
            self.mode = "categorical"
        self._set_color_mapping(color_map)

        patch_collection, gap_patch_collection, annotation_list, horizontal_markers, section_boxes = self._write_patches(max_lines=max_lines)
        print(horizontal_markers)
        
        px = 1/plt.rcParams['figure.dpi']  # pixel in inches
        fig = plt.figure(figsize=(1200*px, 800*px))
        ax = fig.add_subplot(1, 1, 1)
        
        ax.add_collection(patch_collection)
        ax.add_collection(section_boxes)
        # ax.add_collection(gap_patch_collection)
        
        

        # Set axis as flipped - so that we have rtl and top to bottom display
        ax.set_xlim((self.line_length+4) * self.book_count, 0)     # or your max end offset
        
        ax.set_ylim(self.max_lines+10, 0) 

        # Add annotations - below xlim and ylim to allow for wrapping
        self._add_col_annotations(ax, annotation_list)

        # Add the correct legend
        self._build_legend(ax, patch_collection)

        
        ax.set_axis_off()
        
        # Add annotation to bottom for number of chars per row
        if add_explan:
            annotation_text = f"A heatmap of verbatim overlap between {self.book_count} books, where each vertical line is equivalent to {self.line_length} characters"
            ax.text((self.line_length+4) * self.book_count, self.max_lines+10, annotation_text, size=6)

        if self.log:
            log_df = pd.DataFrame(self.patch_log)
            log_df.to_csv("patch_log.csv")
            print(self.max_intensity)

        if export_path is not None:
            fig.savefig(export_path, bbox_inches="tight", dpi=300)
        else:
            plt.show()
